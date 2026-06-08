#include "sqlite3.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define SQLITE_FTS_DDL "CREATE VIRTUAL TABLE docs USING fts5(key UNINDEXED, title, body, tokenize='unicode61 remove_diacritics 2', prefix='5 11')"

typedef struct {
    const char *tsv;
    const char *ops;
    const char *db;
    const char *queries;
    const char *result;
    const char *source_checkout;
    const char *source_version;
    const char *cli_version;
    const char *queries_sha256;
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
    int full_result_us;
    int *runs_us;
    char *error;
    char **keys;
    int key_count;
    char **full_keys;
    int full_key_count;
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

static int batch_has_rowid(sqlite3_int64 *rowids, int rowid_count, sqlite3_int64 rowid) {
    for (int i = 0; i < rowid_count; i++) {
        if (rowids[i] == rowid) {
            return 1;
        }
    }
    return 0;
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

    exec_sql(db, SQLITE_FTS_DDL);
    int64_t start = now_us();
    int in_txn = 0;
    int batch_count = 0;

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(db, "INSERT INTO docs(key, title, body) VALUES (?, ?, ?)", -1, &stmt, NULL);
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

        ensure_txn(db, &in_txn);
        sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, (const char *)title, title_len, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, (const char *)body, body_len, SQLITE_TRANSIENT);
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
            fprintf(stderr, "sqlite-source loaded %d docs\n", *docs);
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

static int64_t sum_text_bytes(sqlite3 *db) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(
        db,
        "SELECT coalesce(sum(length(CAST(title AS BLOB)) + length(CAST(body AS BLOB))), 0) FROM docs",
        -1,
        &stmt,
        NULL
    );
    if (rc != SQLITE_OK) {
        die_sql(db, "prepare sum text bytes", rc);
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        die_sql(db, "sum text bytes", rc);
    }
    int64_t bytes = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);
    return bytes;
}

static void load_ops(
    sqlite3 *db,
    const char *path,
    int *docs,
    int64_t *text_bytes,
    int64_t *load_us,
    int batch_size,
    int *load_batches,
    int *ops_count,
    int *puts,
    int *deletes
) {
    FILE *fh = fopen(path, "rb");
    if (!fh) {
        perror(path);
        exit(1);
    }

    exec_sql(db, SQLITE_FTS_DDL);
    int64_t start = now_us();
    int in_txn = 0;
    int batch_count = 0;
    int rowid_count = 0;
    sqlite3_int64 *batch_rowids = calloc((size_t)batch_size, sizeof(sqlite3_int64));
    if (!batch_rowids) {
        die("out of memory");
    }

    sqlite3_stmt *put_stmt = NULL;
    sqlite3_stmt *delete_stmt = NULL;
    int rc = sqlite3_prepare_v2(
        db,
        "INSERT OR REPLACE INTO docs(rowid, key, title, body) VALUES (?, ?, ?, ?)",
        -1,
        &put_stmt,
        NULL
    );
    if (rc != SQLITE_OK) {
        die_sql(db, "prepare op put", rc);
    }
    rc = sqlite3_prepare_v2(db, "DELETE FROM docs WHERE rowid = ?", -1, &delete_stmt, NULL);
    if (rc != SQLITE_OK) {
        die_sql(db, "prepare op delete", rc);
    }

    char *line = NULL;
    size_t cap = 0;
    ssize_t n;
    while ((n = getline(&line, &cap, fh)) >= 0) {
        while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) {
            line[--n] = '\0';
        }
        if (n == 0) {
            continue;
        }

        char *fields[5] = {0};
        fields[0] = line;
        char *cursor = line;
        for (int i = 1; i < 5; i++) {
            char *tab = strchr(cursor, '\t');
            if (!tab) {
                die("invalid ops line: expected five tab-separated columns");
            }
            *tab = '\0';
            fields[i] = tab + 1;
            cursor = tab + 1;
        }

        char *endptr = NULL;
        sqlite3_int64 rowid = strtoll(fields[1], &endptr, 10);
        if (!fields[1][0] || *endptr != '\0' || rowid <= 0) {
            die("invalid ops rowid");
        }
        if (batch_has_rowid(batch_rowids, rowid_count, rowid)) {
            commit_txn(db, &in_txn, load_batches);
            batch_count = 0;
            rowid_count = 0;
        }

        if (strcmp(fields[0], "put") == 0) {
            int title_len = 0;
            int body_len = 0;
            unsigned char *title = decode_base64(fields[3], strlen(fields[3]), &title_len);
            unsigned char *body = decode_base64(fields[4], strlen(fields[4]), &body_len);
            if (!title || !body) {
                die("invalid base64 put operation");
            }
            ensure_txn(db, &in_txn);
            sqlite3_bind_int64(put_stmt, 1, rowid);
            sqlite3_bind_text(put_stmt, 2, fields[2], -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(put_stmt, 3, (const char *)title, title_len, SQLITE_TRANSIENT);
            sqlite3_bind_text(put_stmt, 4, (const char *)body, body_len, SQLITE_TRANSIENT);
            rc = sqlite3_step(put_stmt);
            if (rc != SQLITE_DONE) {
                die_sql(db, "apply op put", rc);
            }
            sqlite3_reset(put_stmt);
            sqlite3_clear_bindings(put_stmt);
            free(title);
            free(body);
            (*puts)++;
        } else if (strcmp(fields[0], "delete") == 0) {
            ensure_txn(db, &in_txn);
            sqlite3_bind_int64(delete_stmt, 1, rowid);
            rc = sqlite3_step(delete_stmt);
            if (rc != SQLITE_DONE) {
                die_sql(db, "apply op delete", rc);
            }
            sqlite3_reset(delete_stmt);
            sqlite3_clear_bindings(delete_stmt);
            (*deletes)++;
        } else {
            die("invalid ops operation");
        }
        (*ops_count)++;
        batch_count++;
        if (rowid_count < batch_size) {
            batch_rowids[rowid_count++] = rowid;
        }
        maybe_commit_batch(db, batch_size, &batch_count, &in_txn, load_batches);
        if (batch_count == 0) {
            rowid_count = 0;
        }
    }

    free(line);
    free(batch_rowids);
    fclose(fh);
    sqlite3_finalize(put_stmt);
    sqlite3_finalize(delete_stmt);
    commit_txn(db, &in_txn, load_batches);
    exec_sql(db, "PRAGMA optimize");
    *docs = count_docs(db);
    *text_bytes = sum_text_bytes(db);
    *load_us = now_us() - start;
}

static int query_full_result(sqlite3 *db, const char *query, char ***keys, char **error) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(
        db,
        "SELECT key FROM docs WHERE docs MATCH ? ORDER BY key",
        -1,
        &stmt,
        NULL
    );
    if (rc != SQLITE_OK) {
        *error = xstrdup(sqlite3_errmsg(db));
        return 0;
    }
    sqlite3_bind_text(stmt, 1, query, -1, SQLITE_TRANSIENT);
    int count = 0;
    int cap = 0;
    char **out = NULL;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (count == cap) {
            int next_cap = cap ? cap * 2 : 128;
            char **next = realloc(out, (size_t)next_cap * sizeof(char *));
            if (!next) {
                perror("realloc");
                exit(1);
            }
            out = next;
            cap = next_cap;
        }
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

static int query_once(sqlite3 *db, const char *query, int limit, char ***keys, char **error) {
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(
        db,
        "SELECT key FROM docs WHERE docs MATCH ? ORDER BY key LIMIT ?",
        -1,
        &stmt,
        NULL
    );
    if (rc != SQLITE_OK) {
        *error = xstrdup(sqlite3_errmsg(db));
        return 0;
    }
    sqlite3_bind_text(stmt, 1, query, -1, SQLITE_TRANSIENT);
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
    for (int i = 0; i < opts->runs; i++) {
        out.runs_us[i] = 0;
    }
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

    char *full_result_error = NULL;
    int64_t full_result_start = now_us();
    out.total_count = query_full_result(db, query, &out.full_keys, &full_result_error);
    out.full_key_count = out.total_count;
    out.full_result_us = (int)(now_us() - full_result_start);
    if (full_result_error) {
        free(out.error);
        out.error = full_result_error;
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
        size_t need = strlen(opt) + 2;
        if (len + need + 1 > cap) {
            while (len + need + 1 > cap) {
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
        memcpy(out + len, opt, strlen(opt));
        len += strlen(opt);
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
    int ops_count,
    int puts,
    int deletes,
    query_result_t *results,
    size_t result_count
) {
    FILE *fh = fopen(opts->result, "w");
    if (!fh) {
        perror(opts->result);
        exit(1);
    }
    fputs("engine\tmetric\tvalue\tquery\tcount\ttotal_count\truns_us\terror\tkeys\n", fh);
    if (opts->tsv) {
        write_metric(fh, "tsv", opts->tsv);
        if (opts->tsv_sha256) {
            write_metric(fh, "tsv_sha256", opts->tsv_sha256);
        }
    }
    if (opts->ops) {
        write_metric(fh, "ops", opts->ops);
        write_metric_i64(fh, "ops_count", ops_count);
        write_metric_i64(fh, "puts", puts);
        write_metric_i64(fh, "deletes", deletes);
        write_metric(fh, "mutation_contract", "SQLite FTS5 docid INSERT OR REPLACE put; DELETE by docid");
    }
    write_metric(fh, "queries", opts->queries);
    write_metric_i64(fh, "query_count", (int64_t)result_count);
    if (opts->queries_sha256) {
        write_metric(fh, "query_sha256", opts->queries_sha256);
    }
    write_metric_i64(fh, "limit", opts->limit);
    write_metric_i64(fh, "runs", opts->runs);
    write_metric_i64(fh, "warmup", opts->warmup);
    write_metric(fh, "sync_strategy", "PRAGMA synchronous=OFF");
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
    write_metric(
        fh,
        "load_transaction_contract",
        "commit every --batch rows or operations; PRAGMA optimize after load"
    );
    write_metric(fh, "query_connection_contract", "close after load, reopen before timed queries");
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
    write_metric(fh, "sqlite_ddl", SQLITE_FTS_DDL);

    for (size_t i = 0; i < result_count; i++) {
        query_result_t *r = &results[i];
        fputs("sqlite\tfull_result_us\t", fh);
        fprintf(fh, "%d\t", r->full_result_us);
        write_escaped(fh, r->query);
        fprintf(fh, "\t\t%d\t\t", r->total_count);
        write_escaped(fh, r->error);
        fputc('\t', fh);
        char *full_keys_json = json_keys(r->full_keys, r->full_key_count);
        write_escaped(fh, full_keys_json);
        free(full_keys_json);
        fputc('\n', fh);

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
        } else if (strcmp(argv[i], "--ops") == 0) {
            opts.ops = arg_value(argc, argv, &i);
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
        } else if (strcmp(argv[i], "--queries-sha256") == 0) {
            opts.queries_sha256 = arg_value(argc, argv, &i);
        } else if (strcmp(argv[i], "--tsv-sha256") == 0) {
            opts.tsv_sha256 = arg_value(argc, argv, &i);
        } else {
            fprintf(stderr, "unknown argument: %s\n", argv[i]);
            exit(1);
        }
    }
    if ((!opts.tsv && !opts.ops) || !opts.db || !opts.queries || !opts.result) {
        die("required args: (--tsv or --ops) --db --queries --result");
    }
    if (opts.tsv && opts.ops) {
        die("--tsv and --ops are mutually exclusive");
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
    int ops_count = 0;
    int puts = 0;
    int deletes = 0;
    int load_batches = 0;
    if (created) {
        if (opts.ops) {
            load_ops(
                db,
                opts.ops,
                &docs,
                &text_bytes,
                &load_us,
                opts.batch,
                &load_batches,
                &ops_count,
                &puts,
                &deletes
            );
        } else {
            load_tsv(db, opts.tsv, opts.batch, &docs, &text_bytes, &load_us, &load_batches);
            record_meta(db, "tsv_sha256", opts.tsv_sha256);
        }
    } else {
        if (opts.tsv) {
            require_meta(db, "tsv_sha256", opts.tsv_sha256);
        }
        docs = count_docs(db);
        text_bytes = sum_text_bytes(db);
    }
    int64_t close_us = close_sqlite(db, "close sqlite db after load");

    int64_t reopen_start = now_us();
    db = open_configured_sqlite(opts.db);
    int64_t query_reopen_us = now_us() - reopen_start;

    string_list_t queries = read_queries(opts.queries);
    query_result_t *results = xmalloc(queries.count * sizeof(query_result_t));
    for (size_t i = 0; i < queries.count; i++) {
        results[i] = run_query(db, queries.items[i], &opts);
    }

    int64_t query_close_us = close_sqlite(db, "close sqlite db after queries");
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
        ops_count,
        puts,
        deletes,
        results,
        queries.count
    );
    return 0;
}
