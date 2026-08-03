#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <openssl/evp.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    const char *command;
    const char *db;
    const char *tsv;
    const char *queries;
    const char *rank;
    const char *regime;
    int limit;
    int runs;
    int warmups;
    int count;
    int bytes;
} options_t;

typedef struct {
    char *label;
    char *group;
    char *query;
} query_t;

typedef struct {
    query_t *items;
    size_t count;
    size_t capacity;
} query_list_t;

typedef struct {
    int64_t wall_us;
    int total;
    int returned;
    int snippets;
} search_result_t;

static void die(const char *message) {
    fprintf(stderr, "sqlite_bench: %s\n", message);
    exit(2);
}

static void die_sql(sqlite3 *db, const char *message) {
    fprintf(stderr, "sqlite_bench: %s: %s\n", message, sqlite3_errmsg(db));
    exit(2);
}

static void *xmalloc(size_t bytes) {
    void *value = malloc(bytes ? bytes : 1);
    if (!value) {
        perror("malloc");
        exit(2);
    }
    return value;
}

static char *xstrdup(const char *value) {
    char *copy = strdup(value);
    if (!copy) {
        perror("strdup");
        exit(2);
    }
    return copy;
}

static int64_t now_us(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) {
        perror("clock_gettime");
        exit(2);
    }
    return (int64_t)value.tv_sec * 1000000 + value.tv_nsec / 1000;
}

static int compare_i64(const void *left, const void *right) {
    int64_t a = *(const int64_t *)left;
    int64_t b = *(const int64_t *)right;
    return (a > b) - (a < b);
}

static int64_t median(int64_t *values, int count) {
    qsort(values, (size_t)count, sizeof(*values), compare_i64);
    return count ? values[(count - 1) / 2] : 0;
}

static int b64_value(unsigned char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    if (c == '=') return -2;
    return -1;
}

static unsigned char *base64_decode(const char *input, size_t length,
                                    int *output_length) {
    unsigned char *output = xmalloc((length / 4 + 1) * 3);
    int values[4];
    size_t value_count = 0;
    size_t out = 0;
    for (size_t i = 0; i < length; i++) {
        int value = b64_value((unsigned char)input[i]);
        if (value == -1) continue;
        values[value_count++] = value;
        if (value_count != 4) continue;
        if (values[0] < 0 || values[1] < 0) die("invalid base64");
        output[out++] = (unsigned char)((values[0] << 2) | (values[1] >> 4));
        if (values[2] != -2) {
            output[out++] = (unsigned char)((values[1] << 4) | (values[2] >> 2));
            if (values[3] != -2) {
                output[out++] = (unsigned char)((values[2] << 6) | values[3]);
            }
        }
        value_count = 0;
    }
    if (value_count != 0) die("truncated base64");
    *output_length = (int)out;
    return output;
}

static void json_string(const unsigned char *value, int length) {
    putchar('"');
    for (int i = 0; i < length; i++) {
        unsigned char byte = value[i];
        switch (byte) {
        case '"': fputs("\\\"", stdout); break;
        case '\\': fputs("\\\\", stdout); break;
        case '\n': fputs("\\n", stdout); break;
        case '\r': fputs("\\r", stdout); break;
        case '\t': fputs("\\t", stdout); break;
        default:
            if (byte < 32) printf("\\u%04x", byte);
            else putchar(byte);
        }
    }
    putchar('"');
}

static void exec_sql(sqlite3 *db, const char *sql) {
    char *error = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &error) != SQLITE_OK) {
        fprintf(stderr, "sqlite_bench SQL: %s\n", error ? error : "unknown");
        sqlite3_free(error);
        exit(2);
    }
}

static int64_t file_bytes(const char *path) {
    struct stat info;
    return stat(path, &info) == 0 ? (int64_t)info.st_size : 0;
}

static sqlite3 *open_database(const options_t *opts) {
    if (unlink(opts->db) != 0 && errno != ENOENT) {
        perror(opts->db);
        exit(2);
    }
    sqlite3 *db = NULL;
    if (sqlite3_open(opts->db, &db) != SQLITE_OK) die_sql(db, "open");
    exec_sql(db, "PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;"
                 "PRAGMA temp_store=MEMORY; PRAGMA cache_size=-65536;"
                 "PRAGMA mmap_size=0;");
    return db;
}

static void bind_blob(sqlite3_stmt *statement, int index,
                      const unsigned char *value, int length) {
    if (sqlite3_bind_blob(statement, index, value, length, SQLITE_TRANSIENT)
            != SQLITE_OK) {
        die("bind blob failed");
    }
}

static query_list_t read_queries(const char *path) {
    FILE *file = fopen(path, "rb");
    if (!file) {
        perror(path);
        exit(2);
    }
    query_list_t list = {0};
    char *line = NULL;
    size_t capacity = 0;
    ssize_t length;
    while ((length = getline(&line, &capacity, file)) >= 0) {
        while (length > 0 && (line[length - 1] == '\n' || line[length - 1] == '\r'))
            line[--length] = '\0';
        if (!length) continue;
        char *label = line;
        char *group = strchr(label, '\t');
        if (!group) die("invalid query row");
        *group++ = '\0';
        char *ours = strchr(group, '\t');
        if (!ours) die("invalid query row");
        *ours++ = '\0';
        char *sqlite_query = strchr(ours, '\t');
        if (!sqlite_query) die("invalid query row");
        *sqlite_query++ = '\0';
        int decoded_length = 0;
        unsigned char *decoded = base64_decode(
            sqlite_query, strlen(sqlite_query), &decoded_length);
        char *query = xmalloc((size_t)decoded_length + 1);
        memcpy(query, decoded, (size_t)decoded_length);
        query[decoded_length] = '\0';
        free(decoded);
        if (list.count == list.capacity) {
            list.capacity = list.capacity ? list.capacity * 2 : 64;
            list.items = realloc(list.items,
                list.capacity * sizeof(*list.items));
            if (!list.items) die("query allocation failed");
        }
        list.items[list.count++] = (query_t){
            xstrdup(label), xstrdup(group), query
        };
    }
    free(line);
    fclose(file);
    return list;
}

static void free_queries(query_list_t *queries) {
    for (size_t i = 0; i < queries->count; i++) {
        free(queries->items[i].label);
        free(queries->items[i].group);
        free(queries->items[i].query);
    }
    free(queries->items);
}

static void prepare_or_die(sqlite3 *db, const char *sql, sqlite3_stmt **out) {
    if (sqlite3_prepare_v2(db, sql, -1, out, NULL) != SQLITE_OK)
        die_sql(db, "prepare");
}

static const char *retrieval_sql(int ranked) {
    return ranked ?
        "WITH matched AS MATERIALIZED ("
        " SELECT rowid, udi, bm25(docs) score FROM docs WHERE docs MATCH ?1),"
        " ranked AS MATERIALIZED ("
        " SELECT rowid,udi,score,row_number() OVER (PARTITION BY udi"
        " ORDER BY score,rowid) rn FROM matched),"
        " grouped AS MATERIALIZED (SELECT rowid,udi,score FROM ranked WHERE rn=1)"
        " SELECT rowid,udi,score,count(*) OVER() FROM grouped"
        " ORDER BY score,udi LIMIT ?2" :
        "WITH matched AS MATERIALIZED ("
        " SELECT min(rowid) rowid,udi FROM docs WHERE docs MATCH ?1 GROUP BY udi),"
        " grouped AS MATERIALIZED (SELECT rowid,udi,0.0 score FROM matched)"
        " SELECT rowid,udi,score,count(*) OVER() FROM grouped"
        " ORDER BY udi LIMIT ?2";
}

static const char *snippet_sql(int ranked) {
    return ranked ?
        "WITH matched AS MATERIALIZED ("
        " SELECT rowid,udi,bm25(docs) score FROM docs WHERE docs MATCH ?1),"
        " ranked AS MATERIALIZED ("
        " SELECT rowid,udi,score,row_number() OVER (PARTITION BY udi"
        " ORDER BY score,rowid) rn FROM matched),"
        " top AS MATERIALIZED (SELECT rowid,udi,score,count(*) OVER() total"
        " FROM ranked WHERE rn=1 ORDER BY score,udi LIMIT ?2)"
        " SELECT top.rowid,top.udi,top.score,top.total,"
        " snippet(docs,3,'<b>','</b>',' ... ',32)"
        " FROM top JOIN docs ON docs.rowid=top.rowid"
        " WHERE docs MATCH ?1 ORDER BY top.score,top.udi" :
        "WITH matched AS MATERIALIZED ("
        " SELECT min(rowid) rowid,udi FROM docs WHERE docs MATCH ?1 GROUP BY udi),"
        " top AS MATERIALIZED (SELECT rowid,udi,0.0 score,count(*) OVER() total"
        " FROM matched ORDER BY udi LIMIT ?2)"
        " SELECT top.rowid,top.udi,top.score,top.total,"
        " snippet(docs,3,'<b>','</b>',' ... ',32)"
        " FROM top JOIN docs ON docs.rowid=top.rowid"
        " WHERE docs MATCH ?1 ORDER BY top.udi";
}

static search_result_t execute_search(sqlite3 *db, sqlite3_stmt *statement,
                                      const char *query, int limit,
                                      int with_snippets) {
    sqlite3_reset(statement);
    sqlite3_clear_bindings(statement);
    if (sqlite3_bind_text(statement, 1, query, -1, SQLITE_TRANSIENT) != SQLITE_OK ||
        sqlite3_bind_int(statement, 2, limit) != SQLITE_OK) {
        die_sql(db, "bind search");
    }
    search_result_t result = {0};
    int64_t started = now_us();
    int rc;
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        result.returned++;
        result.total = sqlite3_column_int(statement, 3);
        if (with_snippets && sqlite3_column_bytes(statement, 4) > 0)
            result.snippets++;
    }
    result.wall_us = now_us() - started;
    if (rc != SQLITE_DONE) die_sql(db, "step search");
    return result;
}

static void result_set_sha(sqlite3 *db, sqlite3_stmt *statement,
                           const char *query, char output[65]) {
    sqlite3_reset(statement);
    sqlite3_clear_bindings(statement);
    if (sqlite3_bind_text(statement, 1, query, -1, SQLITE_TRANSIENT) != SQLITE_OK)
        die_sql(db, "bind set query");
    EVP_MD_CTX *context = EVP_MD_CTX_new();
    if (!context || EVP_DigestInit_ex(context, EVP_sha256(), NULL) != 1)
        die("sha256 init failed");
    int rc;
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        const unsigned char *udi = sqlite3_column_blob(statement, 0);
        int bytes = sqlite3_column_bytes(statement, 0);
        char length[32];
        int prefix = snprintf(length, sizeof(length), "%d:", bytes);
        EVP_DigestUpdate(context, length, (size_t)prefix);
        EVP_DigestUpdate(context, udi, (size_t)bytes);
        EVP_DigestUpdate(context, "\n", 1);
    }
    if (rc != SQLITE_DONE) die_sql(db, "step set query");
    unsigned char digest[32];
    unsigned int digest_bytes = 0;
    EVP_DigestFinal_ex(context, digest, &digest_bytes);
    EVP_MD_CTX_free(context);
    for (unsigned int i = 0; i < digest_bytes; i++)
        sprintf(output + i * 2, "%02x", digest[i]);
    output[64] = '\0';
}

static void run_fts(const options_t *opts) {
    sqlite3 *db = open_database(opts);
    exec_sql(db,
        "CREATE VIRTUAL TABLE docs USING fts5("
        "chunk_key UNINDEXED,udi UNINDEXED,content_version UNINDEXED,content,"
        "tokenize='unicode61 remove_diacritics 1',prefix='5 11')");
    sqlite3_stmt *insert = NULL;
    prepare_or_die(db, "INSERT INTO docs(chunk_key,udi,content_version,content)"
        " VALUES(?1,?2,?3,?4)", &insert);
    FILE *file = fopen(opts->tsv, "rb");
    if (!file) { perror(opts->tsv); exit(2); }
    char *line = NULL;
    size_t line_capacity = 0;
    ssize_t line_length;
    int documents = 0;
    int64_t text_bytes = 0;
    int64_t ingest_started = now_us();
    exec_sql(db, "BEGIN IMMEDIATE");
    while ((line_length = getline(&line, &line_capacity, file)) >= 0) {
        while (line_length > 0 && (line[line_length - 1] == '\n' ||
                                  line[line_length - 1] == '\r'))
            line[--line_length] = '\0';
        if (!line_length) continue;
        char *key64 = line;
        char *udi64 = strchr(key64, '\t');
        if (!udi64) die("invalid corpus row");
        *udi64++ = '\0';
        char *version = strchr(udi64, '\t');
        if (!version) die("invalid corpus row");
        *version++ = '\0';
        char *content64 = strchr(version, '\t');
        if (!content64) die("invalid corpus row");
        *content64++ = '\0';
        int key_bytes, udi_bytes, content_bytes;
        unsigned char *key = base64_decode(key64, strlen(key64), &key_bytes);
        unsigned char *udi = base64_decode(udi64, strlen(udi64), &udi_bytes);
        unsigned char *content = base64_decode(
            content64, strlen(content64), &content_bytes);
        sqlite3_reset(insert);
        sqlite3_clear_bindings(insert);
        bind_blob(insert, 1, key, key_bytes);
        bind_blob(insert, 2, udi, udi_bytes);
        sqlite3_bind_int64(insert, 3, strtoll(version, NULL, 10));
        bind_blob(insert, 4, content, content_bytes);
        if (sqlite3_step(insert) != SQLITE_DONE) die_sql(db, "insert");
        free(key); free(udi); free(content);
        documents++;
        text_bytes += content_bytes;
    }
    exec_sql(db, "COMMIT");
    int64_t insert_us = now_us() - ingest_started;
    int64_t optimize_started = now_us();
    exec_sql(db, "INSERT INTO docs(docs) VALUES('optimize')");
    int64_t optimize_us = now_us() - optimize_started;
    sqlite3_finalize(insert);
    free(line);
    fclose(file);

    int ranked = strcmp(opts->rank, "bm25") == 0;
    sqlite3_stmt *retrieval = NULL, *snippet = NULL, *set_query = NULL;
    prepare_or_die(db, retrieval_sql(ranked), &retrieval);
    prepare_or_die(db, snippet_sql(ranked), &snippet);
    prepare_or_die(db,
        "SELECT udi FROM docs WHERE docs MATCH ?1 GROUP BY udi ORDER BY udi",
        &set_query);
    query_list_t queries = read_queries(opts->queries);
    printf("{\"schema_version\":1,\"engine\":\"sqlite\","
           "\"subcommand\":\"fts\",\"rank\":");
    json_string((const unsigned char *)opts->rank, (int)strlen(opts->rank));
    printf(",\"regime\":\"served\",\"limit\":%d,\"documents\":%d,"
           "\"text_bytes\":%lld,\"ingest\":{\"insert_us\":%lld,"
           "\"optimize_us\":%lld},\"store_bytes\":%lld,\"cases\":[",
           opts->limit, documents, (long long)text_bytes, (long long)insert_us,
           (long long)optimize_us, (long long)file_bytes(opts->db));
    for (size_t query_index = 0; query_index < queries.count; query_index++) {
        query_t *query = &queries.items[query_index];
        for (int i = 0; i < opts->warmups; i++) {
            execute_search(db, retrieval, query->query, opts->limit, 0);
            execute_search(db, snippet, query->query, opts->limit, 1);
        }
        int64_t *retrieval_samples = xmalloc((size_t)opts->runs * sizeof(int64_t));
        int64_t *full_samples = xmalloc((size_t)opts->runs * sizeof(int64_t));
        search_result_t final_retrieval = {0}, final_full = {0};
        for (int i = 0; i < opts->runs; i++) {
            final_retrieval = execute_search(
                db, retrieval, query->query, opts->limit, 0);
            final_full = execute_search(
                db, snippet, query->query, opts->limit, 1);
            retrieval_samples[i] = final_retrieval.wall_us;
            full_samples[i] = final_full.wall_us;
        }
        int64_t retrieval_us = median(retrieval_samples, opts->runs);
        int64_t combined_us = median(full_samples, opts->runs);
        int64_t hydration_us = combined_us > retrieval_us ?
            combined_us - retrieval_us : 0;
        char set_sha[65];
        result_set_sha(db, set_query, query->query, set_sha);
        if (query_index) putchar(',');
        fputs("{\"label\":", stdout);
        json_string((const unsigned char *)query->label,
            (int)strlen(query->label));
        fputs(",\"group\":", stdout);
        json_string((const unsigned char *)query->group,
            (int)strlen(query->group));
        fputs(",\"query\":", stdout);
        json_string((const unsigned char *)query->query,
            (int)strlen(query->query));
        printf(",\"limit\":%d,\"total\":%d,\"returned\":%d,\"snippet_rows\":%d,"
               "\"snippet_content_verified\":%d,"
               "\"result_sha256\":\"%s\",\"retrieval_us\":%lld,"
               "\"hydration_us\":%lld,\"combined_us\":%lld,"
               "\"marginal_us_per_row\":%.9g}",
               opts->limit, final_retrieval.total, final_retrieval.returned,
               final_full.snippets, final_full.snippets, set_sha,
               (long long)retrieval_us, (long long)hydration_us,
               (long long)combined_us,
               final_full.returned ?
                   (double)combined_us / final_full.returned : 0.0);
        free(retrieval_samples);
        free(full_samples);
    }
    puts("]}");
    free_queries(&queries);
    sqlite3_finalize(retrieval);
    sqlite3_finalize(snippet);
    sqlite3_finalize(set_query);
    sqlite3_close(db);
}

static void run_index(const options_t *opts) {
    sqlite3 *db = open_database(opts);
    exec_sql(db, "CREATE TABLE kv(k INTEGER PRIMARY KEY,v BLOB,idx INTEGER);"
                 "CREATE INDEX kv_idx ON kv(idx)");
    sqlite3_stmt *insert = NULL;
    prepare_or_die(db, "INSERT INTO kv VALUES(?1,?2,?3)", &insert);
    int64_t started = now_us();
    exec_sql(db, "BEGIN");
    for (int i = 1; i <= opts->count; i++) {
        sqlite3_reset(insert);
        sqlite3_bind_int(insert, 1, i);
        sqlite3_bind_int64(insert, 2, i);
        sqlite3_bind_int(insert, 3, i % 1000);
        if (sqlite3_step(insert) != SQLITE_DONE) die_sql(db, "index insert");
    }
    exec_sql(db, "COMMIT");
    int64_t put_us = now_us() - started;
    sqlite3_finalize(insert);
    sqlite3_stmt *query = NULL;
    prepare_or_die(db, "SELECT count(*) FROM kv WHERE idx BETWEEN 100 AND 199", &query);
    started = now_us();
    if (sqlite3_step(query) != SQLITE_ROW) die_sql(db, "index query");
    int returned = sqlite3_column_int(query, 0);
    int64_t query_us = now_us() - started;
    sqlite3_finalize(query);
    printf("{\"schema_version\":1,\"engine\":\"sqlite\","
           "\"subcommand\":\"index\",\"count\":%d,\"put_us\":%lld,"
           "\"query_us\":%lld,\"returned\":%d,\"store_bytes\":%lld}\n",
           opts->count, (long long)put_us, (long long)query_us, returned,
           (long long)file_bytes(opts->db));
    sqlite3_close(db);
}

static void run_heads(const options_t *opts) {
    sqlite3 *db = open_database(opts);
    exec_sql(db, "CREATE TABLE heads(k INTEGER PRIMARY KEY,v BLOB)");
    sqlite3_stmt *insert = NULL, *query = NULL;
    prepare_or_die(db, "INSERT INTO heads VALUES(?1,?2)", &insert);
    prepare_or_die(db, "SELECT v FROM heads WHERE k=?1", &query);
    exec_sql(db, "BEGIN");
    for (int i = 1; i <= opts->count; i++) {
        sqlite3_reset(insert);
        sqlite3_bind_int(insert, 1, i);
        sqlite3_bind_int64(insert, 2, i);
        if (sqlite3_step(insert) != SQLITE_DONE) die_sql(db, "heads insert");
    }
    exec_sql(db, "COMMIT");
    int batches[] = {1, 20, 200, 1000};
    fputs("{\"schema_version\":1,\"engine\":\"sqlite\","
          "\"subcommand\":\"heads\",\"rows\":[", stdout);
    for (size_t b = 0; b < sizeof(batches) / sizeof(batches[0]); b++) {
        int batch = batches[b] < opts->count ? batches[b] : opts->count;
        int64_t started = now_us();
        for (int i = 1; i <= batch; i++) {
            sqlite3_reset(query);
            sqlite3_bind_int(query, 1, i);
            if (sqlite3_step(query) != SQLITE_ROW) die_sql(db, "heads query");
        }
        if (b) putchar(',');
        printf("{\"batch\":%d,\"returned\":%d,\"wall_us\":%lld}",
            batch, batch, (long long)(now_us() - started));
    }
    printf("],\"count\":%d,\"store_bytes\":%lld}\n", opts->count,
        (long long)file_bytes(opts->db));
    sqlite3_finalize(insert);
    sqlite3_finalize(query);
    sqlite3_close(db);
}

static void run_compression(const options_t *opts) {
    sqlite3 *db = open_database(opts);
    exec_sql(db, "CREATE TABLE payloads(k INTEGER PRIMARY KEY,v BLOB)");
    sqlite3_stmt *insert = NULL;
    prepare_or_die(db, "INSERT INTO payloads VALUES(?1,?2)", &insert);
    unsigned char *payload = xmalloc((size_t)opts->bytes);
    const char *pattern = "compressible-benchmark-payload-";
    for (int i = 0; i < opts->bytes; i++) payload[i] = pattern[i % 31];
    int64_t started = now_us();
    exec_sql(db, "BEGIN");
    for (int i = 1; i <= opts->count; i++) {
        sqlite3_reset(insert);
        sqlite3_bind_int(insert, 1, i);
        bind_blob(insert, 2, payload, opts->bytes);
        if (sqlite3_step(insert) != SQLITE_DONE) die_sql(db, "payload insert");
    }
    exec_sql(db, "COMMIT");
    int64_t put_us = now_us() - started;
    sqlite3_finalize(insert);
    free(payload);
    printf("{\"schema_version\":1,\"engine\":\"sqlite\","
           "\"subcommand\":\"compression\",\"method\":\"none\","
           "\"count\":%d,\"value_bytes\":%d,\"put_us\":%lld,"
           "\"store_bytes\":%lld}\n", opts->count, opts->bytes,
           (long long)put_us, (long long)file_bytes(opts->db));
    sqlite3_close(db);
}

static options_t parse_options(int argc, char **argv) {
    if (argc < 2) die("missing subcommand");
    options_t opts = {
        .command = argv[1], .rank = "bm25", .regime = "served",
        .limit = 20, .runs = 7, .warmups = 3,
        .count = 10000, .bytes = 1024
    };
    for (int i = 2; i < argc; i += 2) {
        if (i + 1 >= argc) die("flag missing value");
        const char *flag = argv[i], *value = argv[i + 1];
        if (!strcmp(flag, "--db")) opts.db = value;
        else if (!strcmp(flag, "--tsv")) opts.tsv = value;
        else if (!strcmp(flag, "--queries")) opts.queries = value;
        else if (!strcmp(flag, "--rank")) opts.rank = value;
        else if (!strcmp(flag, "--regime")) opts.regime = value;
        else if (!strcmp(flag, "--limit")) opts.limit = atoi(value);
        else if (!strcmp(flag, "--runs")) opts.runs = atoi(value);
        else if (!strcmp(flag, "--warmups")) opts.warmups = atoi(value);
        else if (!strcmp(flag, "--count")) opts.count = atoi(value);
        else if (!strcmp(flag, "--bytes")) opts.bytes = atoi(value);
        else die("unknown flag");
    }
    if (!opts.db) die("--db is required");
    if (!strcmp(opts.command, "fts") && (!opts.tsv || !opts.queries))
        die("fts requires --tsv and --queries");
    return opts;
}

int main(int argc, char **argv) {
    options_t opts = parse_options(argc, argv);
    if (!strcmp(opts.command, "fts")) run_fts(&opts);
    else if (!strcmp(opts.command, "index")) run_index(&opts);
    else if (!strcmp(opts.command, "heads")) run_heads(&opts);
    else if (!strcmp(opts.command, "compression")) run_compression(&opts);
    else die("unknown subcommand");
    return 0;
}
