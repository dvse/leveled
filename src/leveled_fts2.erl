%% Internal FTS2 facade.  leveled_fts remains the only public search API.

-module(leveled_fts2).

-export([
    available/2,
    publish/5,
    publish_documents/3,
    export_documents/3,
    lookup_documents/3,
    consolidate/2,
    consolidate/3,
    search/5,
    search_dirty/5,
    search_dirty/6,
    posting_read/6,
    posting_read_dirty/6
]).

available(Bookie, Schema) ->
    leveled_fts2_search:root(Bookie, Schema).

publish(Bookie, Schema, TokenDocs, CandidateRecords, HitRecords) ->
    leveled_fts2_build:publish(
        Bookie, Schema, TokenDocs, CandidateRecords, HitRecords
    ).

publish_documents(Bookie, Schema, Documents) ->
    leveled_fts2_build:publish_documents(Bookie, Schema, Documents).

export_documents(Bookie, Schema, Root) ->
    leveled_fts2_search:export_documents(Bookie, Schema, Root).

lookup_documents(Bookie, Schema, SourceIds) ->
    leveled_fts2_search:lookup_documents(Bookie, Schema, SourceIds).

consolidate(Bookie, Schema) ->
    leveled_fts2_build:consolidate(Bookie, Schema).

consolidate(Bookie, Schema, Hook) ->
    leveled_fts2_build:consolidate(Bookie, Schema, Hook).

search(Bookie, Schema, Root, AST, Opts) ->
    leveled_fts2_search:search(Bookie, Schema, Root, AST, Opts).

search_dirty(Bookie, Schema, Root, AST, Opts) ->
    leveled_fts2_search:search_dirty(Bookie, Schema, Root, AST, Opts).

search_dirty(Bookie, Schema, Root, AST, Opts, Hook) ->
    leveled_fts2_search:search_dirty(Bookie, Schema, Root, AST, Opts, Hook).

posting_read(Bookie, Schema, Root, AST, SourceIds, Opts) ->
    leveled_fts2_search:posting_read(
        Bookie, Schema, Root, AST, SourceIds, Opts
    ).

posting_read_dirty(Bookie, Schema, Root, AST, SourceIds, Opts) ->
    leveled_fts2_search:posting_read_dirty(
        Bookie, Schema, Root, AST, SourceIds, Opts
    ).
