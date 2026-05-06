// SPDX-License-Identifier: MIT
pragma solidity ^0.8.30;

import {Script} from "forge-std/Script.sol";
import {stdJson} from "forge-std/StdJson.sol";
import {LightClient} from "../LightClient.sol";

contract DeployLightClient is Script {
    using stdJson for string;

    function run() external returns (LightClient lc) {
        string memory path = vm.envString("LIGHT_CLIENT_ARGS_JSON_FILE");
        string memory json = vm.readFile(path);

        address[] memory validators = json.readAddressArray(".validators");
        uint64[] memory weights = _readUint64Array(json, ".weights");
        bytes32 adminChainId = json.readBytes32(".admin_chain_id");
        uint32 epoch = uint32(vm.parseUint(json.readString(".epoch")));

        require(validators.length > 0, "validators empty");
        require(validators.length == weights.length, "validators/weights length mismatch");

        vm.broadcast();
        lc = new LightClient(validators, weights, adminChainId, epoch);

        require(lc.adminChainId() == adminChainId, "post-deploy admin chain mismatch");
    }

    function _readUint64Array(string memory json, string memory key)
        internal
        pure
        returns (uint64[] memory out)
    {
        uint256[] memory raw = json.readUintArray(key);
        out = new uint64[](raw.length);
        for (uint256 i = 0; i < raw.length; i++) {
            require(raw[i] <= type(uint64).max, "weight overflow");
            out[i] = uint64(raw[i]);
        }
    }
}
