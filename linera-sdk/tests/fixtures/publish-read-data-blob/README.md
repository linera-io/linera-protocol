# Publish and read a data blob

This example shows how to create a blob and how to read it.
This example should be read in conjunction with the client
test `run_test_publish_read_data_blob`.

It shows 3 scenarios:
* Publishing and reading blobs with the publishing and reading in different
blocks.
* Publishing and reading blobs in the same transaction.
* Publishing and reqding blobs in the same block but not the same transaction
