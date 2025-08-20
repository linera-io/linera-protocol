# Publish and read a data blob

This example shows how to create a blob and how to read it.
This example should be read in conjunction with the end-to-end
test `test_wasm_end_to_end_publish_read_data_blob`.

It shows 3 scenarios:
* Publishing and reading blobs with the publishing and reading in different
blocks.
* Publishing and reading blobs in the same transaction.
* Publishing and reqding blobs in the same block but not the same transaction
