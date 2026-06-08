#include "sqlite3.h"

#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define INDEX_DDL "CREATE TABLE docs(key TEXT PRIMARY KEY, title BLOB NOT NULL, body BLOB NOT NULL, mod100 INTEGER NOT NULL, mod10 INTEGER NOT NULL, mod2 INTEGER NOT NULL); CREATE INDEX docs_mod100_key_idx ON docs(mod100, key); CREATE INDEX docs_mod10_key_idx ON docs(mod10, key); CREATE INDEX docs_mod2_key_idx ON docs(mod2, key)"
#define INDEX_CONTRACT "mod100,mod10,mod2 from document ordinal"

typedef struct {
    const char *tsv;
    const char *db;
    const char *queries;
    const char *result;
    const char *source_checkout;
    const char *source_version;
    const char *cli_version;
    const char *tsv_sha256;
    int batch;
    int limit;
    int runs;
    int warmup;
    int force;
} options_t;

typedef struct {
    char **items;
    size_t count;
    size_t cap;
} string_list_t;

typedef struct {
    char *query;
    int count;
    int total_count;
    int *runs_us;
    char *error;
    char **keys;
    int key_count;
} query_result_t;

static void die(const char *msg) {
    fprintf(stderr, "%s\n", msg);
    exit(1);
}

static void die_sql(sqlite3 *db, const char *msg, int rc) {
    fprintf(stderr, "%s: %s (%d)\n", msg, sqlite3_errmsg(db), rc);
    exit(1);
}

static int64_t now_us(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime");
        exit(1);
    }
    return ((int64_t)ts.tv_sec * 1000000) + (ts.tv_nsec / 1000);
}

static char *xstrdup(const char *s) {
    char *out = strdup(s);
    if (!out) {
        perror("strdup");
        exit(1);
    }
    return out;
}

static void *xmalloc(size_t size) {
    void *out = malloc(size ? size : 1);
    if (!out) {
        perror("malloc");
        exit(1);
    }
    return out;
}

static void list_append(string_list_t *list, const char *value) {
    if (list->count == list->cap) {
        size_t next = list->cap ? list->cap * 2 : 16;
        char **items = realloc(list->items, next * sizeof(char *));
        if (!items) {
            perror("realloc");
            exit(1);
        }
        list->items = items;
        list->cap = next;
    }
    list->items[list->count++] = xstrdup(value);
}

static char *trim(char *s) {
    while (*s && isspace((unsigned char)*s)) {
        s++;
    }
    size_t len = strlen(s);
    while (len > 0 && isspace((unsigned char)s[len - 1])) {
        s[--len] = '\0';
    }
    return s;
}

static string_list_t read_queries(const char *path) {
    FILE *fh = fopen(path, "r");
    if (!fh) {
        perror(path);
        exit(1);
    }
    string_list_t out = {0};
    char *line = NULL;
    size_t cap = 0;
    ssize_t n;
    while ((n = getline(&line, &cap, fh)) >= 0) {
        (void)n;
        char *q = trim(line);
        if (*q == '\0' || *q == '#') {
            continue;
        }
        list_append(&out, q);
    }
    free(line);
    fclose(fh);
    return out;
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

static unsigned char *decode_base64(const char *in, size_t len, int *out_len) {
    unsigned char *out = xmalloc((len / 4 + 1) * 3);
    size_t oi = 0;
    int vals[4];
    size_t vi = 0;

    for (size_t i = 0; i < len; i++) {
        int v = b64_value((unsigned char)in[i]);
        if (v == -1) {
            continue;
        }
        vals[vi++] = v;
        if (vi != 4) {
            continue;
        }
        if (vals[0] < 0 || vals[1] < 0) {
            free(out);
            return NULL;
        }
        out[oi++] = (unsigned char)((vals[0] << 2) | (vals[1] >> 4));
        if (vals[2] != -2) {
            if (vals[2] < 0) {
                free(out);
                return NULL;
            }
            out[oi++] = (unsigned char)(((vals[1] & 15) << 4) | (vals[2] >> 2));
        }
        if (vals[3] != -2) {
            if (vals[2] < 0 || vals[3] < 0) {
                free(out);
                return NULL;
            }
            out[oi++] = (unsigned char)(((vals[2] & 3) << 6) | vals[3]);
        }
        vi = 0;
    }
    if (vi != 0) {
        free(out);
        return NULL;
    }
    *out_len = (int)oi;
    return out;
}

static void exec_sql(sqlite3 *db, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "%s: %s\n", sql, err ? err : sqlite3_errmsg(db));
        sqlite3_free(err);
        exit(1);
    }
}

static void configure_sqlite(sqlite3 *db) {
    exec_sql(db, "PRAGMA journal_mode=OFF");
    exec_sql(db, "PRAGMA synchronous=OFF");
    exec_sql(db, "PRAGMA temp_store=MEMORY");
    exec_sql(db, "PRAGMA cache_size=-200000");
}

static sqlite3 *open_configured_sqlite(const char *path) {
    sqlite3 *db = NULL;
    int rc = sqlite3_open(path, &db);
    if (rc != SQLITE_OK) {
        die_sql(db, "open sqlite db", rc);
    }
    configure_sqlite(db);
    return db;
}

static int64_t close_sqlite(sqlite3 *db, const char *stage) {
    int64_t start = now_us();
    int rc = sqlite3_close(db);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "%s: sqlite3_close failed (%d)\n", stage, rc);
        exit(1);
    }
    return now_us() - start;
}

static void ensure_txn(sqlite3 *db, int *in_txn) {
    if (!*in_txn) {
        exec_sql(db, "BEGIN");
        *in_txn = 1;
    }
}

static void commit_txn(sqlite3 *db, int *in_txn, int *batches) {
    if (*in_txn) {
        exec_sql(db, "COMMIT");
        *in_txn = 0;
        (*batches)++;
    }
}

static void maybe_commit_batch(
    sqlite3 *db,
    int batch_size,
    int *batch_count,
    int *in_txn,
    int *batches
) {
    if (batch_size > 0 && *batch_count >= batch_size) {
        commit_txn(db, in_txn, batches);
        *batch_count = 0;
    }
}

static int64_t file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        return 0;
    }
    return (int64_t)st.st_size;
}

static void record_meta(sqlite3 *db, const char *key, const char *value) {
    if (!value) {
        return;
    }
    exec_sql(db, "CREATE TABLE IF NOT EXISTS bench_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)");
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(
        db,
        "INSERT OR REPLACE INTO bench_meta(key, value) VALUES (?, ?)",
        -1,
        &stmt,
        NULL
    );
    if (rc != SQLITE_OK) {
        die_sql(db, "prepare record meta", rc);
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, value, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        die_sql(db, "record meta", rc);
    }
    sqlite3_finalize(stmt);
}

static void require_meta(sqlite3 *db, const char *key, const char *expected) {
    if (!expected) {
        die("missing expected SQLite reuse metadata");
    }
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(
        db,
        "SELECT value FROM bench_meta WHERE key = ?",
        -1,
        &stmt,
        NULL
    );
    if (rc != SQLITE_OK) {
        die_sql(db, "prepare read meta", rc);
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        die("SQLite DB missing benchmark metadata; rebuild without --reuse-sqlite");
    }
    const unsigned char *actual = sqlite3_column_text(stmt, 0);
    if (!actual || strcmp((const char *)actual, expected) != 0) {
        fprintf(
            stderr,
            "SQLite DB metadata mismatch for %s: expected %s actual %s\n",
            key,
            expected,
            actual ? (const char *)actual : "(null)"
        );
        sqlite3_finalize(stmt);
        exit(1);
    }
    sqlite3_finalize(stmt);
}

static void load_tsv(
    sqlite3 *db,
    const char *path,
    int batch_size,
    int *docs,
    int64_t *text_bytes,
    int64_t *load_us,
    int *load_batches
) {
    FILE *fh = fopen(path, "rb");
    if (!fh) {
        perror(path);
        exit(1);
    }

    exec_sql(db, INDEX_DDL);
    int64_t start = now_us();
    int batch_count = 0;
    int in_txn = 0;

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(
        db,
        "INSERT INTO docs(key, title, body, mod100, mod10, mod2) VALUES (?, ?, ?, ?, ?, ?)",
        -1,
        &stmt,
        NULL
    );
    if (rc != SQLITE_OK) {
        die_sql(db, "prepare insert", rc);
    }

    char *line = NULL;
    size_t cap = 0;
    ssize_t n;
    while ((n = getline(&line, &cap, fh)) >= 0) {
        while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) {
            line[--n] = '\0';
        }
        char *tab1 = memchr(line, '\t', (size_t)n);
        if (!tab1) {
            die("invalid TSV line: missing first tab");
        }
        char *tab2 = memchr(tab1 + 1, '\t', (size_t)(line + n - tab1 - 1));
        if (!tab2) {
            die("invalid TSV line: missing second tab");
        }
        *tab1 = '\0';
        *tab2 = '\0';
        char *key = line;
        char *title64 = tab1 + 1;
        char *body64 = tab2 + 1;

        int title_len = 0;
        int body_len = 0;
        unsigned char *title = decode_base64(title64, strlen(title64), &title_len);
        unsigned char *body = decode_base64(body64, strlen(body64), &body_len);
        if (!title || !body) {
            die("invalid base64 document row");
        }

        int doc_number = *docs + 1;
        ensure_txn(db, &in_txn);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 2, title, title_len, SQLITE_TRANSIENT);
        sqlite3_bind_blob(stmt, 3, body, body_len, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 4, doc_number % 100);
        sqlite3_bind_int(stmt, 5, doc_number % 10);
        sqlite3_bind_int(stmt, 6, doc_number % 2);
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            die_sql(db, "insert row", rc);
        }
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        free(title);
        free(body);

        (*docs)++;
        batch_count++;
        *text_bytes += title_len + body_len;
        maybe_commit_batch(db, batch_size, &batch_count, &in_txn, load_batches);
        if ((*docs % 10000) == 0) {
            fprintf(stderr, "sqlite-index loaded %d docs\n", *docs);
        }
    }

    free(line);
    fclose(fh);
    sqlite3_finalize(stmt);
    commit_txn(db, &in_txn, load_batches);
    exec_sql(db, "PRAGMA optimize");
    *load_us = now_us() - start;
}

static int count_docs(sqlite3 *db) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM docs", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        die_sql(db, "prepare count docs", rc);
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        die_sql(db, "count docs", rc);
    }
    int docs = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return docs;
}

static int parse_query(const char *query, const char **column, int *value, char **error) {
    const char *colon = strchr(query, ':');
    if (!colon || colon == query || colon[1] == '\0') {
        *error = xstrdup("invalid_index_query");
        return 0;
    }

    size_t name_len = (size_t)(colon - query);
    if (name_len == 6 && strncmp(query, "mod100", name_len) == 0) {
        *column = "mod100";
    } else if (name_len == 5 && strncmp(query, "mod10", name_len) == 0) {
        *column = "mod10";
    } else if (name_len == 4 && strncmp(query, "mod2", name_len) == 0) {
        *column = "mod2";
    } else {
        *error = xstrdup("unknown_index_query");
        return 0;
    }

    errno = 0;
    char *end = NULL;
    long parsed = strtol(colon + 1, &end, 10);
    if (errno != 0 || *end != '\0' || parsed < 0 || parsed > 1000000) {
        *error = xstrdup("invalid_index_value");
        return 0;
    }
    *value = (int)parsed;
    return 1;
}

static int query_count(sqlite3 *db, const char *query, char **error) {
    const char *column = NULL;
    int value = 0;
    if (!parse_query(query, &column, &value, error)) {
        return 0;
    }

    char sql[128];
    snprintf(sql, sizeof(sql), "SELECT count(*) FROM docs WHERE %s = ?", column);
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        *error = xstrdup(sqlite3_errmsg(db));
        return 0;
    }
    sqlite3_bind_int(stmt, 1, value);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        *error = xstrdup(sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 0;
    }
    int count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return count;
}

static int query_once(sqlite3 *db, const char *query, int limit, char ***keys, char **error) {
    const char *column = NULL;
    int value = 0;
    if (!parse_query(query, &column, &value, error)) {
        return 0;
    }

    char sql[160];
    snprintf(sql, sizeof(sql), "SELECT key FROM docs WHERE %s = ? ORDER BY key LIMIT ?", column);
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        *error = xstrdup(sqlite3_errmsg(db));
        return 0;
    }
    sqlite3_bind_int(stmt, 1, value);
    sqlite3_bind_int(stmt, 2, limit);

    char **out = xmalloc((size_t)(limit > 0 ? limit : 1) * sizeof(char *));
    int count = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char *key = sqlite3_column_text(stmt, 0);
        out[count++] = xstrdup(key ? (const char *)key : "");
    }
    if (rc != SQLITE_DONE) {
        *error = xstrdup(sqlite3_errmsg(db));
        for (int i = 0; i < count; i++) {
            free(out[i]);
        }
        free(out);
        sqlite3_finalize(stmt);
        return 0;
    }
    sqlite3_finalize(stmt);
    *keys = out;
    return count;
}

static void free_keys(char **keys, int count) {
    if (!keys) return;
    for (int i = 0; i < count; i++) {
        free(keys[i]);
    }
    free(keys);
}

static int keys_equal(char **a, int a_count, char **b, int b_count) {
    if (a_count != b_count) return 0;
    for (int i = 0; i < a_count; i++) {
        if (strcmp(a[i], b[i]) != 0) return 0;
    }
    return 1;
}

static query_result_t run_query(sqlite3 *db, const char *query, const options_t *opts) {
    query_result_t out = {0};
    out.query = xstrdup(query);
    out.runs_us = xmalloc((size_t)opts->runs * sizeof(int));
    out.error = xstrdup("none");

    for (int i = 0; i < opts->warmup; i++) {
        char **keys = NULL;
        char *error = NULL;
        int count = query_once(db, query, opts->limit, &keys, &error);
        free_keys(keys, count);
        free(error);
    }

    for (int i = 0; i < opts->runs; i++) {
        char **keys = NULL;
        char *error = NULL;
        int64_t start = now_us();
        int count = query_once(db, query, opts->limit, &keys, &error);
        int64_t elapsed = now_us() - start;
        out.runs_us[i] = (int)elapsed;

        if (error) {
            free(out.error);
            out.error = error;
            free_keys(keys, count);
            continue;
        }

        if (i == 0) {
            out.keys = keys;
            out.key_count = count;
            out.count = count;
        } else {
            if (!keys_equal(out.keys, out.key_count, keys, count)) {
                free(out.error);
                out.error = xstrdup("inconsistent_timed_results");
            }
            free_keys(keys, count);
        }
    }
    char *count_error = NULL;
    out.total_count = query_count(db, query, &count_error);
    if (count_error) {
        free(out.error);
        out.error = count_error;
    }
    return out;
}

static void write_escaped(FILE *fh, const char *s) {
    int quote = 0;
    for (const char *p = s; *p; p++) {
        if (*p == '\t' || *p == '\n' || *p == '\r' || *p == '"') {
            quote = 1;
            break;
        }
    }
    if (!quote) {
        fputs(s, fh);
        return;
    }
    fputc('"', fh);
    for (const char *p = s; *p; p++) {
        if (*p == '"') {
            fputc('"', fh);
        }
        fputc(*p, fh);
    }
    fputc('"', fh);
}

static void write_metric(FILE *fh, const char *metric, const char *value) {
    fputs("sqlite\t", fh);
    write_escaped(fh, metric);
    fputc('\t', fh);
    write_escaped(fh, value);
    fputs("\t\t\t\t\t\t\n", fh);
}

static void write_metric_i64(FILE *fh, const char *metric, int64_t value) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%lld", (long long)value);
    write_metric(fh, metric, buf);
}

static char *sqlite_compile_options_csv(void) {
    size_t cap = 1024;
    size_t len = 0;
    char *out = xmalloc(cap);
    out[0] = '\0';

    for (int i = 0;; i++) {
        const char *opt = sqlite3_compileoption_get(i);
        if (!opt) {
            break;
        }
        size_t opt_len = strlen(opt);
        if (len + opt_len + 2 > cap) {
            while (len + opt_len + 2 > cap) {
                cap *= 2;
            }
            char *next = realloc(out, cap);
            if (!next) {
                perror("realloc");
                exit(1);
            }
            out = next;
        }
        if (len > 0) {
            out[len++] = ',';
        }
        memcpy(out + len, opt, opt_len);
        len += opt_len;
        out[len] = '\0';
    }
    return out;
}

static void append_char(char **buf, size_t *len, size_t *cap, char c) {
    if (*len + 2 > *cap) {
        while (*len + 2 > *cap) {
            *cap *= 2;
        }
        char *next = realloc(*buf, *cap);
        if (!next) {
            perror("realloc");
            exit(1);
        }
        *buf = next;
    }
    (*buf)[(*len)++] = c;
    (*buf)[*len] = '\0';
}

static void append_text(char **buf, size_t *len, size_t *cap, const char *text) {
    while (*text) {
        append_char(buf, len, cap, *text++);
    }
}

static char *json_keys(char **keys, int count) {
    size_t cap = 128;
    size_t len = 0;
    char *out = xmalloc(cap);
    out[0] = '\0';
    append_char(&out, &len, &cap, '[');
    for (int i = 0; i < count; i++) {
        if (i) append_char(&out, &len, &cap, ',');
        append_char(&out, &len, &cap, '"');
        const unsigned char *p = (const unsigned char *)keys[i];
        while (*p) {
            unsigned char c = *p++;
            switch (c) {
                case '"': append_text(&out, &len, &cap, "\\\""); break;
                case '\\': append_text(&out, &len, &cap, "\\\\"); break;
                case '\b': append_text(&out, &len, &cap, "\\b"); break;
                case '\f': append_text(&out, &len, &cap, "\\f"); break;
                case '\n': append_text(&out, &len, &cap, "\\n"); break;
                case '\r': append_text(&out, &len, &cap, "\\r"); break;
                case '\t': append_text(&out, &len, &cap, "\\t"); break;
                default:
                    if (c < 0x20) {
                        char esc[7];
                        snprintf(esc, sizeof(esc), "\\u%04x", c);
                        append_text(&out, &len, &cap, esc);
                    } else {
                        append_char(&out, &len, &cap, (char)c);
                    }
                    break;
            }
        }
        append_char(&out, &len, &cap, '"');
    }
    append_char(&out, &len, &cap, ']');
    return out;
}

static void write_results(
    const options_t *opts,
    int docs,
    int64_t text_bytes,
    int64_t load_us,
    int64_t store_bytes,
    int load_batches,
    int64_t close_us,
    int64_t query_reopen_us,
    int64_t query_close_us,
    query_result_t *results,
    size_t result_count
) {
    FILE *fh = fopen(opts->result, "w");
    if (!fh) {
        perror(opts->result);
        exit(1);
    }
    fputs("engine\tmetric\tvalue\tquery\tcount\ttotal_count\truns_us\terror\tkeys\n", fh);
    write_metric(fh, "tsv", opts->tsv);
    if (opts->tsv_sha256) {
        write_metric(fh, "tsv_sha256", opts->tsv_sha256);
    }
    write_metric_i64(fh, "docs", docs);
    write_metric_i64(fh, "text_bytes", text_bytes);
    write_metric_i64(fh, "load_us", load_us);
    write_metric_i64(fh, "load_finalized_us", load_us + close_us);
    write_metric_i64(fh, "load_reopened_us", load_us + close_us + query_reopen_us);
    write_metric_i64(fh, "load_batch_size", opts->batch);
    write_metric_i64(fh, "load_batches", load_batches);
    write_metric_i64(fh, "close_us", close_us);
    write_metric_i64(fh, "query_reopen_us", query_reopen_us);
    write_metric_i64(fh, "query_close_us", query_close_us);
    write_metric_i64(fh, "store_bytes", store_bytes);
    write_metric(fh, "sqlite_journal_mode", "OFF");
    write_metric(fh, "sqlite_synchronous", "OFF");
    write_metric(fh, "sqlite_temp_store", "MEMORY");
    write_metric(fh, "sqlite_cache_size", "-200000");
    write_metric(fh, "sqlite_runtime_version", sqlite3_libversion());
    write_metric(fh, "sqlite_runtime_sourceid", sqlite3_sourceid());
    write_metric(fh, "sqlite_runner", "source_amalgamation_c");
    char *compile_options = sqlite_compile_options_csv();
    write_metric(fh, "sqlite_compile_options", compile_options);
    free(compile_options);
    write_metric(fh, "sqlite_source_checkout", opts->source_checkout);
    write_metric(fh, "sqlite_source_version", opts->source_version);
    write_metric(fh, "sqlite_cli_version", opts->cli_version);
    write_metric(fh, "sqlite_query_order", "ORDER BY key");
    write_metric(fh, "sqlite_ddl", INDEX_DDL);
    write_metric(fh, "benchmark_mode", "secondary_index");
    write_metric(fh, "index_contract", INDEX_CONTRACT);
    write_metric(fh, "load_transaction_contract", "commit every --batch rows");

    for (size_t i = 0; i < result_count; i++) {
        query_result_t *r = &results[i];
        fputs("sqlite\tquery_us\t\t", fh);
        write_escaped(fh, r->query);
        fprintf(fh, "\t%d\t%d\t", r->count, r->total_count);
        for (int j = 0; j < opts->runs; j++) {
            if (j) fputc(',', fh);
            fprintf(fh, "%d", r->runs_us[j]);
        }
        fputc('\t', fh);
        write_escaped(fh, r->error);
        fputc('\t', fh);
        char *keys_json = json_keys(r->keys, r->key_count);
        write_escaped(fh, keys_json);
        free(keys_json);
        fputc('\n', fh);
    }
    fclose(fh);
}

static const char *arg_value(int argc, char **argv, int *i) {
    if (*i + 1 >= argc) {
        die("missing argument value");
    }
    return argv[++(*i)];
}

static options_t parse_args(int argc, char **argv) {
    options_t opts = {
        .batch = 200,
        .limit = 20,
        .runs = 5,
        .warmup = 1,
        .source_checkout = "/Users/dvse/repos/sqlite",
        .source_version = "unknown",
        .cli_version = "unknown",
    };
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--tsv") == 0) {
            opts.tsv = arg_value(argc, argv, &i);
        } else if (strcmp(argv[i], "--db") == 0) {
            opts.db = arg_value(argc, argv, &i);
        } else if (strcmp(argv[i], "--queries") == 0) {
            opts.queries = arg_value(argc, argv, &i);
        } else if (strcmp(argv[i], "--result") == 0) {
            opts.result = arg_value(argc, argv, &i);
        } else if (strcmp(argv[i], "--batch") == 0) {
            opts.batch = atoi(arg_value(argc, argv, &i));
        } else if (strcmp(argv[i], "--limit") == 0) {
            opts.limit = atoi(arg_value(argc, argv, &i));
        } else if (strcmp(argv[i], "--runs") == 0) {
            opts.runs = atoi(arg_value(argc, argv, &i));
        } else if (strcmp(argv[i], "--warmup") == 0) {
            opts.warmup = atoi(arg_value(argc, argv, &i));
        } else if (strcmp(argv[i], "--force") == 0) {
            opts.force = 1;
        } else if (strcmp(argv[i], "--source-checkout") == 0) {
            opts.source_checkout = arg_value(argc, argv, &i);
        } else if (strcmp(argv[i], "--source-version") == 0) {
            opts.source_version = arg_value(argc, argv, &i);
        } else if (strcmp(argv[i], "--cli-version") == 0) {
            opts.cli_version = arg_value(argc, argv, &i);
        } else if (strcmp(argv[i], "--tsv-sha256") == 0) {
            opts.tsv_sha256 = arg_value(argc, argv, &i);
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            exit(1);
        }
    }
    if (!opts.tsv || !opts.db || !opts.queries || !opts.result) {
        die("required args: --tsv --db --queries --result");
    }
    if (opts.batch < 1 || opts.limit < 0 || opts.runs < 1 || opts.warmup < 0) {
        die("invalid batch/limit/runs/warmup");
    }
    return opts;
}

int main(int argc, char **argv) {
    options_t opts = parse_args(argc, argv);
    if (opts.force) {
        unlink(opts.db);
    }

    int created = access(opts.db, F_OK) != 0;
    sqlite3 *db = open_configured_sqlite(opts.db);

    int docs = 0;
    int64_t text_bytes = 0;
    int64_t load_us = 0;
    int load_batches = 0;
    if (created) {
        load_tsv(db, opts.tsv, opts.batch, &docs, &text_bytes, &load_us, &load_batches);
        record_meta(db, "tsv_sha256", opts.tsv_sha256);
    } else {
        require_meta(db, "tsv_sha256", opts.tsv_sha256);
        docs = count_docs(db);
    }
    int64_t close_us = close_sqlite(db, "close sqlite index db after load");

    int64_t reopen_start = now_us();
    db = open_configured_sqlite(opts.db);
    int64_t query_reopen_us = now_us() - reopen_start;

    string_list_t queries = read_queries(opts.queries);
    query_result_t *results = xmalloc(queries.count * sizeof(query_result_t));
    for (size_t i = 0; i < queries.count; i++) {
        results[i] = run_query(db, queries.items[i], &opts);
    }

    int64_t query_close_us = close_sqlite(db, "close sqlite index db after queries");
    write_results(
        &opts,
        docs,
        text_bytes,
        load_us,
        file_size(opts.db),
        load_batches,
        close_us,
        query_reopen_us,
        query_close_us,
        results,
        queries.count
    );
    return 0;
}
