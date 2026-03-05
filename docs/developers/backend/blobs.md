# Using Data Blobs

Some applications may want to use static assets, like images or other data: e.g.
the `non-fungible` example application implements NFTs, and each NFT has an
associated image.

Data blobs are pieces of binary data that, once published on _any_ chain, can be
used on _all_ chains. What format they are in and what they are used for is
determined by the application(s) that read(s) them.

You can use the `linera publish-data-blob` command to publish the contents of a
file, as an operation in a block on one of your chains. This will print the ID
of the new blob, including its hash. Alternatively, you can run `linera service`
and use the `publishDataBlob` GraphQL mutation.

Applications can now use `runtime.read_data_blob(blob_hash)` to read the blob.
This works on any chain, not only the one that published it. The first time your
client executes a block reading a blob, it will download the blob from the
validators if it doesn't already have it locally.

In the case of the NFT app, it is only the service, not the contract, that
actually uses the blob data to display it as an image in the frontend. But we
still want to make sure that the user has the image locally as soon as they
receive an NFT, even if they don't view it yet. This can be achieved by calling
`runtime.assert_data_blob_exists(blob_hash)` in the contract: It will make sure
the data is available, without actually loading it.

For the complete code please take a look at the [`non-fungible`
contract](https://github.com/linera-io/linera-protocol/blob/{{#include
../../RELEASE_HASH}}/examples/non-fungible/src/contract.rs) and
[service](https://github.com/linera-io/linera-protocol/blob/{{#include
../../RELEASE_HASH}}/examples/non-fungible/src/service.rs).
