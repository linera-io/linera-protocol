// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pragma solidity ^0.8.0;

import "./LineraTypes.sol";

// The "LineraTypes.sol" is created via the command
// cargo run -p serde-generate-bin -- --language solidity LineraTypes.yaml > LineraTypes.sol
// from the package "serde-reflection" commit 95e57c4c2df2fc7215e627da353071a2cb91fdcb

// This library provides Linera functionalities to EVM contracts
// It should not be modified.

library Linera {

    function chain_id() internal returns (LineraTypes.ChainId memory) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_chain_id();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return LineraTypes.bcs_deserialize_ChainId(output);
    }

    function application_creator_chain_id() internal returns (LineraTypes.ChainId memory) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_application_creator_chain_id();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return LineraTypes.bcs_deserialize_ChainId(output);
    }

    function chain_ownership() internal returns (LineraTypes.ChainOwnership memory) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_chain_ownership();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return LineraTypes.bcs_deserialize_ChainOwnership(output);
    }

    function read_data_blob(bytes32 hash) internal returns (bytes memory) {
        address precompile = address(0x0b);
        LineraTypes.CryptoHash memory hash2 = LineraTypes.CryptoHash(hash);
        LineraTypes.BaseRuntimePrecompile_ReadDataBlob memory read_data_blob_ = LineraTypes.BaseRuntimePrecompile_ReadDataBlob(hash2);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_read_data_blob(read_data_blob_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return output;
    }

    function assert_data_blob_exists(bytes32 hash) internal {
        address precompile = address(0x0b);
        LineraTypes.CryptoHash memory hash2 = LineraTypes.CryptoHash(hash);
        LineraTypes.BaseRuntimePrecompile_AssertDataBlobExists memory assert_data_blob_exists_ = LineraTypes.BaseRuntimePrecompile_AssertDataBlobExists(hash2);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_assert_data_blob_exists(assert_data_blob_exists_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        assert(output.length == 0);
    }

    function try_call_application(bytes32 universal_address, bytes memory operation) internal returns (bytes memory) {
        address precompile = address(0x0b);
        LineraTypes.ApplicationId memory target = LineraTypes.ApplicationId(LineraTypes.CryptoHash(universal_address));
        LineraTypes.ContractRuntimePrecompile_TryCallApplication memory try_call_application_ = LineraTypes.ContractRuntimePrecompile_TryCallApplication(target, operation);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_try_call_application(try_call_application_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return output;
    }

    function validation_round() internal returns (LineraTypes.opt_uint32 memory) {
        address precompile = address(0x0b);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_validation_round();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return LineraTypes.bcs_deserialize_opt_uint32(output);
    }

    function send_message(bytes32 chain_id1, bytes memory message) internal {
        address precompile = address(0x0b);
        LineraTypes.ChainId memory chain_id2 = LineraTypes.ChainId(LineraTypes.CryptoHash(chain_id1));
        LineraTypes.ContractRuntimePrecompile_TryCallApplication memory try_call_application_;
        LineraTypes.ContractRuntimePrecompile_SendMessage memory send_message_ = LineraTypes.ContractRuntimePrecompile_SendMessage(chain_id2, message);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_send_message(send_message_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        require(output.length == 0);
    }

    function message_id() internal returns (LineraTypes.opt_MessageId memory) {
        address precompile = address(0x0b);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_message_id();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return LineraTypes.bcs_deserialize_opt_MessageId(output);
    }

    function message_is_bouncing() internal returns (LineraTypes.MessageIsBouncing memory) {
        address precompile = address(0x0b);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_message_is_bouncing();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return LineraTypes.bcs_deserialize_MessageIsBouncing(output);
    }

    function linera_emit(bytes memory stream_name, bytes memory value) internal returns (uint32) {
        address precompile = address(0x0b);
        LineraTypes.StreamName memory stream_name2 = LineraTypes.StreamName(stream_name);
        LineraTypes.ContractRuntimePrecompile_Emit memory emit_ = LineraTypes.ContractRuntimePrecompile_Emit(stream_name2, value);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_emit(emit_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return LineraTypes.bcs_deserialize_uint32(output);
    }

    function read_event(bytes32 chain_id1, bytes memory stream_name, uint32 index) internal returns (bytes memory) {
        address precompile = address(0x0b);
        LineraTypes.ChainId memory chain_id2 = LineraTypes.ChainId(LineraTypes.CryptoHash(chain_id1));
        LineraTypes.StreamName memory stream_name2 = LineraTypes.StreamName(stream_name);
        LineraTypes.ContractRuntimePrecompile_ReadEvent memory read_event_ = LineraTypes.ContractRuntimePrecompile_ReadEvent(chain_id2, stream_name2, index);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_read_event(read_event_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return output;
    }

    function subscribe_to_events(bytes32 chain_id1, bytes32 application_id, bytes memory stream_name) internal {
        address precompile = address(0x0b);
        LineraTypes.ChainId memory chain_id2 = LineraTypes.ChainId(LineraTypes.CryptoHash(chain_id1));
        LineraTypes.ApplicationId memory application_id2 = LineraTypes.ApplicationId(LineraTypes.CryptoHash(application_id));
        LineraTypes.StreamName memory stream_name2 = LineraTypes.StreamName(stream_name);
        LineraTypes.ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events_ = LineraTypes.ContractRuntimePrecompile_SubscribeToEvents(chain_id2, application_id2, stream_name2);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_subscribe_to_events(subscribe_to_events_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        require(output.length == 0);
    }

    function unsubscribe_from_events(bytes32 chain_id1, bytes32 application_id, bytes memory stream_name) internal {
        address precompile = address(0x0b);
        LineraTypes.ChainId memory chain_id2 = LineraTypes.ChainId(LineraTypes.CryptoHash(chain_id1));
        LineraTypes.ApplicationId memory application_id2 = LineraTypes.ApplicationId(LineraTypes.CryptoHash(application_id));
        LineraTypes.StreamName memory stream_name2 = LineraTypes.StreamName(stream_name);
        LineraTypes.ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events_ = LineraTypes.ContractRuntimePrecompile_UnsubscribeFromEvents(chain_id2, application_id2, stream_name2);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_unsubscribe_from_events(unsubscribe_from_events_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        require(output.length == 0);
    }

    function try_query_application(bytes32 universal_address, bytes memory argument) internal returns (bytes memory) {
        address precompile = address(0x0b);
        LineraTypes.ApplicationId memory target = LineraTypes.ApplicationId(LineraTypes.CryptoHash(universal_address));
        LineraTypes.ServiceRuntimePrecompile_TryQueryApplication memory try_query_application_ = LineraTypes.ServiceRuntimePrecompile_TryQueryApplication(target, argument);
        LineraTypes.ServiceRuntimePrecompile memory service = LineraTypes.ServiceRuntimePrecompile_case_try_query_application(try_query_application_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_service(service);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return output;
    }
}
