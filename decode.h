#ifndef DECODE_H_
#define DECODE_H_

void ppl_insert(LogicalDecodingContext *ctx, Relation relation, HeapTuple tuple);
void ppl_delete(LogicalDecodingContext *ctx, Relation relation, HeapTuple tuple);
void ppl_update(LogicalDecodingContext *ctx, Relation relation, HeapTuple oldtuple, HeapTuple newtuple);

#endif
