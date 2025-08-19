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

    // Exported types

    struct ChainId {
        bytes32 value;
    }

    function chainid_from(LineraTypes.ChainId memory entry)
        internal
        pure
        returns (ChainId memory)
    {
        return ChainId(entry.value.value);
    }

    struct AccountOwner {
        uint8 choice;
        // choice=0 corresponds to Reserved
        uint8 reserved;
        // choice=1 corresponds to Address32
        bytes32 address32;
        // choice=2 corresponds to Address20
        bytes20 address20;
    }

    function accountowner_from(LineraTypes.AccountOwner memory owner)
        internal
        pure
        returns (AccountOwner memory)
    {
        return AccountOwner(owner.choice, owner.reserved, owner.address32.value, owner.address20);
    }

    function accountowner_to(Linera.AccountOwner memory owner)
        internal
        pure
        returns (LineraTypes.AccountOwner memory)
    {
        LineraTypes.CryptoHash memory hash = LineraTypes.CryptoHash(owner.address32);
        return LineraTypes.AccountOwner(owner.choice, owner.reserved, hash, owner.address20);
    }

    struct AccountOwnerBalance {
        AccountOwner account_owner;
        uint256 balance;
    }

    function accountownerbalance_from(LineraTypes.AccountOwnerBalanceInner memory entry)
        internal
        pure
        returns (AccountOwnerBalance memory)
    {
        uint256 balance = uint256(entry.balance_.value);
        AccountOwner memory account_owner = accountowner_from(entry.account_owner);
        return AccountOwnerBalance(account_owner, balance);
    }

    struct TimeDelta {
        uint64 value;
    }

    function timedelta_from(LineraTypes.TimeDelta memory entry)
        internal
        pure
        returns (TimeDelta memory)
    {
        return TimeDelta(entry.value);
    }

    struct opt_TimeDelta {
        bool has_value;
        uint64 value;
    }

    function opt_timedelta_from(LineraTypes.opt_TimeDelta memory entry)
        internal
        pure
        returns (opt_TimeDelta memory)
    {
        return opt_TimeDelta(entry.has_value, entry.value.value);
    }

    struct TimeoutConfig {
        opt_TimeDelta fast_round_duration;
        TimeDelta base_timeout;
        TimeDelta timeout_increment;
        TimeDelta fallback_duration;
    }

    function timeoutconfig_from(LineraTypes.TimeoutConfig memory entry)
        internal
        pure
        returns (TimeoutConfig memory)
    {
        return TimeoutConfig(opt_timedelta_from(entry.fast_round_duration),
                             timedelta_from(entry.base_timeout),
                             timedelta_from(entry.timeout_increment),
                             timedelta_from(entry.fallback_duration));
    }

    struct AccountOwnerWeight {
        Linera.AccountOwner account_owner;
        uint64 weight;
    }

    function accountownerweight_from(LineraTypes.key_values_AccountOwner_uint64 memory entry)
        internal
        pure
        returns (AccountOwnerWeight memory)
    {
        return AccountOwnerWeight(accountowner_from(entry.key), entry.value);
    }

    struct ChainOwnership {
        AccountOwner[] super_owners;
        AccountOwnerWeight[] owners;
        uint32 multi_leader_rounds;
        bool open_multi_leader_rounds;
        TimeoutConfig timeout_config;
    }

    function chainownership_from(LineraTypes.ChainOwnership memory entry)
        internal
        pure
        returns (ChainOwnership memory)
    {
        uint256 len1 = entry.super_owners.length;
        AccountOwner[] memory super_owners;
        super_owners = new AccountOwner[](len1);
        for (uint256 i=0; i<len1; i++) {
            super_owners[i] = accountowner_from(entry.super_owners[i]);
        }
        uint256 len2 = entry.owners.length;
        AccountOwnerWeight[] memory owners;
        owners = new AccountOwnerWeight[](len2);
        for (uint256 i=0; i<len2; i++) {
            owners[i] = accountownerweight_from(entry.owners[i]);
        }
        return ChainOwnership(super_owners, owners, entry.multi_leader_rounds, entry.open_multi_leader_rounds, timeoutconfig_from(entry.timeout_config));
    }

    struct opt_uint32 {
        bool has_value;
        uint32 value;
    }

    function opt_uint32_from(LineraTypes.opt_uint32 memory entry)
        internal
        pure
        returns (opt_uint32 memory)
    {
        return opt_uint32(entry.has_value, entry.value);
    }

    struct ApplicationId {
        bytes32 application_description_hash;
    }

    function applicationid_from(LineraTypes.ApplicationId memory entry)
        internal
        pure
        returns (ApplicationId memory)
    {
        return ApplicationId(entry.application_description_hash.value);
    }

    struct opt_ApplicationId {
        bool has_value;
        ApplicationId value;
    }

    function opt_applicationid_from(LineraTypes.opt_ApplicationId memory entry)
        internal
        pure
        returns (opt_ApplicationId memory)
    {
        return opt_ApplicationId(entry.has_value, applicationid_from(entry.value));
    }

    struct opt_AccountOwner {
        bool has_value;
        AccountOwner value;
    }

    struct opt_ChainId {
        bool has_value;
        ChainId value;
    }

    function opt_accountowner_from(LineraTypes.opt_AccountOwner memory entry)
        internal
        pure
        returns (opt_AccountOwner memory)
    {
        return opt_AccountOwner(entry.has_value, accountowner_from(entry.value));
    }

    function opt_chainid_from(LineraTypes.opt_ChainId memory entry)
        internal
        pure
        returns (opt_ChainId memory)
    {
        return opt_ChainId(entry.has_value, chainid_from(entry.value));
    }

    function opt_chainid_none()
        internal
        pure
        returns (opt_ChainId memory)
    {
        return opt_ChainId(false, ChainId(bytes32(0)));
    }

    enum OptionBool { None, True, False }

    function optionbool_from(LineraTypes.MessageIsBouncing memory entry)
        internal
        pure
        returns (OptionBool)
    {
        if (entry.value == LineraTypes.OptionBool.True) {
            return OptionBool.True;
        }
        if (entry.value == LineraTypes.OptionBool.False) {
            return OptionBool.False;
        }
        return OptionBool.None;
    }



    struct StreamUpdate {
        ChainId chain_id;
        StreamId stream_id;
        uint32 previous_index;
        uint32 next_index;
    }

    struct StreamId {
        GenericApplicationId application_id;
        StreamName stream_name;
    }

    struct StreamName {
        bytes value;
    }

    struct GenericApplicationId {
        uint8 choice;
        // choice=0 corresponds to System
        // choice=1 corresponds to User
        ApplicationId user;
    }

    // BaseRuntime functions

    function chain_id() internal returns (Linera.ChainId memory) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_chain_id();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        LineraTypes.ChainId memory output2 = LineraTypes.bcs_deserialize_ChainId(output1);
        return chainid_from(output2);
    }

    function block_height() internal returns (uint64) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_block_height();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        LineraTypes.BlockHeight memory output2 = LineraTypes.bcs_deserialize_BlockHeight(output);
        return output2.value;
    }

    function application_creator_chain_id() internal returns (ChainId memory) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_application_creator_chain_id();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        LineraTypes.ChainId memory output2 = LineraTypes.bcs_deserialize_ChainId(output1);
        return ChainId(output2.value.value);
    }

    function read_system_timestamp() internal returns (uint64) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_read_system_timestamp();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        LineraTypes.Timestamp memory output2 = LineraTypes.bcs_deserialize_Timestamp(output);
        return output2.value;
    }

    function read_chain_balance() internal returns (uint256) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_read_chain_balance();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        LineraTypes.Amount memory output2 = LineraTypes.bcs_deserialize_Amount(output);
        return uint256(output2.value);
    }

    function read_owner_balance(Linera.AccountOwner memory owner) internal returns (uint256) {
        address precompile = address(0x0b);
        LineraTypes.AccountOwner memory owner2 = accountowner_to(owner);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_read_owner_balance(owner2);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        LineraTypes.Amount memory output2 = LineraTypes.bcs_deserialize_Amount(output);
        return uint256(output2.value);
    }

    function read_owner_balances() internal returns (Linera.AccountOwnerBalance[] memory) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_read_owner_balances();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        LineraTypes.ResponseReadOwnerBalances memory output2 = LineraTypes.bcs_deserialize_ResponseReadOwnerBalances(output);
        uint256 len = output2.value.length;
        Linera.AccountOwnerBalance[] memory elist;
        elist = new Linera.AccountOwnerBalance[](len);
        for (uint256 i=0; i<len; i++) {
            elist[i] = accountownerbalance_from(output2.value[i]);
        }
        return elist;
    }

    function read_balance_owners() internal returns (Linera.AccountOwner[] memory result) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_read_balance_owners();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        LineraTypes.ResponseReadBalanceOwners memory output2 = LineraTypes.bcs_deserialize_ResponseReadBalanceOwners(output1);
        uint256 len = output2.value.length;
        Linera.AccountOwner[] memory elist;
        elist = new Linera.AccountOwner[](len);
        for (uint256 i=0; i<len; i++) {
            elist[i] = accountowner_from(output2.value[i]);
        }
        return elist;
    }

    function chain_ownership() internal returns (Linera.ChainOwnership memory) {
        address precompile = address(0x0b);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_chain_ownership();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        LineraTypes.ChainOwnership memory output2 = LineraTypes.bcs_deserialize_ChainOwnership(output1);
        return chainownership_from(output2);
    }

    function read_data_blob(bytes32 hash) internal returns (bytes memory) {
        address precompile = address(0x0b);
        LineraTypes.CryptoHash memory hash2 = LineraTypes.CryptoHash(hash);
        LineraTypes.DataBlobHash memory hash3 = LineraTypes.DataBlobHash(hash2);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_read_data_blob(hash3);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return output;
    }

    function assert_data_blob_exists(bytes32 hash) internal {
        address precompile = address(0x0b);
        LineraTypes.CryptoHash memory hash2 = LineraTypes.CryptoHash(hash);
        LineraTypes.DataBlobHash memory hash3 = LineraTypes.DataBlobHash(hash2);
        LineraTypes.BaseRuntimePrecompile memory base = LineraTypes.BaseRuntimePrecompile_case_assert_data_blob_exists(hash3);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_base(base);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        assert(output.length == 0);
    }

    // ContractRuntime functions

    function authenticated_signer() internal returns (Linera.opt_AccountOwner memory) {
        address precompile = address(0x0b);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_authenticated_signer();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        LineraTypes.opt_AccountOwner memory output2 = LineraTypes.bcs_deserialize_opt_AccountOwner(output1);
        return opt_accountowner_from(output2);
    }

    function message_origin_chain_id() internal returns (opt_ChainId memory) {
        address precompile = address(0x0b);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_message_origin_chain_id();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        LineraTypes.opt_ChainId memory output2 = LineraTypes.bcs_deserialize_opt_ChainId(output1);
        return opt_chainid_from(output2);
    }

    function message_is_bouncing() internal returns (OptionBool) {
        address precompile = address(0x0b);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_message_is_bouncing();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        LineraTypes.MessageIsBouncing memory output2 = LineraTypes.bcs_deserialize_MessageIsBouncing(output1);
        return optionbool_from(output2);
    }

    function authenticated_caller_id() internal returns (Linera.opt_ApplicationId memory) {
        address precompile = address(0x0b);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_authenticated_caller_id();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output1) = precompile.call(input2);
        require(success);
        LineraTypes.opt_ApplicationId memory output2 = LineraTypes.bcs_deserialize_opt_ApplicationId(output1);
        return opt_applicationid_from(output2);
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

    function subscribe_to_events(bytes32 chain_id1, bytes32 subscribed_application_id, bytes memory stream_name) internal {
        address precompile = address(0x0b);
        LineraTypes.ChainId memory chain_id2 = LineraTypes.ChainId(LineraTypes.CryptoHash(chain_id1));
        LineraTypes.ApplicationId memory application_id2 = LineraTypes.ApplicationId(LineraTypes.CryptoHash(subscribed_application_id));
        LineraTypes.StreamName memory stream_name2 = LineraTypes.StreamName(stream_name);
        LineraTypes.ContractRuntimePrecompile_SubscribeToEvents memory subscribe_to_events_ = LineraTypes.ContractRuntimePrecompile_SubscribeToEvents(chain_id2, application_id2, stream_name2);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_subscribe_to_events(subscribe_to_events_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        require(output.length == 0);
    }

    function unsubscribe_from_events(bytes32 chain_id1, bytes32 unsubscribe_application_id, bytes memory stream_name) internal {
        address precompile = address(0x0b);
        LineraTypes.ChainId memory chain_id2 = LineraTypes.ChainId(LineraTypes.CryptoHash(chain_id1));
        LineraTypes.ApplicationId memory application_id2 = LineraTypes.ApplicationId(LineraTypes.CryptoHash(unsubscribe_application_id));
        LineraTypes.StreamName memory stream_name2 = LineraTypes.StreamName(stream_name);
        LineraTypes.ContractRuntimePrecompile_UnsubscribeFromEvents memory unsubscribe_from_events_ = LineraTypes.ContractRuntimePrecompile_UnsubscribeFromEvents(chain_id2, application_id2, stream_name2);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_unsubscribe_from_events(unsubscribe_from_events_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        require(output.length == 0);
    }

    function query_service(bytes32 universal_address, bytes memory query) internal returns (bytes memory) {
        address precompile = address(0x0b);
        LineraTypes.ApplicationId memory target = LineraTypes.ApplicationId(LineraTypes.CryptoHash(universal_address));
        LineraTypes.ContractRuntimePrecompile_QueryService memory query_service_ = LineraTypes.ContractRuntimePrecompile_QueryService(target, query);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_query_service(query_service_);
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        return output;
    }

    function validation_round() internal returns (Linera.opt_uint32 memory) {
        address precompile = address(0x0b);
        LineraTypes.ContractRuntimePrecompile memory contract_ = LineraTypes.ContractRuntimePrecompile_case_validation_round();
        LineraTypes.RuntimePrecompile memory input1 = LineraTypes.RuntimePrecompile_case_contract(contract_);
        bytes memory input2 = LineraTypes.bcs_serialize_RuntimePrecompile(input1);
        (bool success, bytes memory output) = precompile.call(input2);
        require(success);
        LineraTypes.opt_uint32 memory output2 = LineraTypes.bcs_deserialize_opt_uint32(output);
        return opt_uint32_from(output2);
    }

    // ServiceRuntime functions.

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
