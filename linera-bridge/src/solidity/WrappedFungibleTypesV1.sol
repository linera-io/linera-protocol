/// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;
import "BridgeTypes.sol";

library WrappedFungibleTypesV1 {
    function bcs_serialize_uleb128(uint256 x) internal pure returns (bytes memory) {
        bytes memory result;
        bytes1 entry;
        while (true) {
            if (x < 128) {
                entry = bytes1(uint8(x));
                return abi.encodePacked(result, entry);
            } else {
                uint256 xb = x >> 7;
                uint256 remainder = x - (xb << 7);
                require(remainder < 128);
                entry = bytes1(uint8(remainder) + 128);
                result = abi.encodePacked(result, entry);
                x = xb;
            }
        }
        require(false, "This line is unreachable");
        return result;
    }

    function bcs_deserialize_offset_uleb128(uint256 pos, bytes memory input) internal pure returns (uint256, uint256) {
        uint256 idx = 0;
        while (true) {
            if (uint8(input[pos + idx]) < 128) {
                uint256 result = 0;
                uint256 power = 1;
                for (uint256 u = 0; u < idx; u++) {
                    uint8 val = uint8(input[pos + u]) - 128;
                    result += power * uint256(val);
                    power *= 128;
                }
                result += power * uint8(input[pos + idx]);
                return (pos + idx + 1, result);
            }
            idx += 1;
        }
        require(false, "This line is unreachable");
        return (0, 0);
    }

    struct Account {
        BridgeTypes.ChainId chain_id;
        BridgeTypes.AccountOwner owner;
    }

    function bcs_serialize_Account(Account memory input) internal pure returns (bytes memory) {
        bytes memory result = BridgeTypes.bcs_serialize_ChainId(input.chain_id);
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_AccountOwner(input.owner));
    }

    function bcs_deserialize_offset_Account(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Account memory)
    {
        uint256 new_pos;
        BridgeTypes.ChainId memory chain_id;
        (new_pos, chain_id) = BridgeTypes.bcs_deserialize_offset_ChainId(pos, input);
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(new_pos, input);
        return (new_pos, Account(chain_id, owner));
    }

    function bcs_deserialize_Account(bytes memory input) internal pure returns (Account memory) {
        uint256 new_pos;
        Account memory value;
        (new_pos, value) = bcs_deserialize_offset_Account(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct ApplicationId {
        BridgeTypes.CryptoHash application_description_hash;
    }

    function bcs_serialize_ApplicationId(ApplicationId memory input) internal pure returns (bytes memory) {
        return BridgeTypes.bcs_serialize_CryptoHash(input.application_description_hash);
    }

    function bcs_deserialize_offset_ApplicationId(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, ApplicationId memory)
    {
        uint256 new_pos;
        BridgeTypes.CryptoHash memory application_description_hash;
        (new_pos, application_description_hash) = BridgeTypes.bcs_deserialize_offset_CryptoHash(pos, input);
        return (new_pos, ApplicationId(application_description_hash));
    }

    function bcs_deserialize_ApplicationId(bytes memory input) internal pure returns (ApplicationId memory) {
        uint256 new_pos;
        ApplicationId memory value;
        (new_pos, value) = bcs_deserialize_offset_ApplicationId(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct BurnEvent {
        bytes20 target;
        uint128 amount;
    }

    function bcs_serialize_BurnEvent(BurnEvent memory input) internal pure returns (bytes memory) {
        bytes memory result = bcs_serialize_bytes20(input.target);
        return abi.encodePacked(result, bcs_serialize_uint128(input.amount));
    }

    function bcs_deserialize_offset_BurnEvent(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, BurnEvent memory)
    {
        uint256 new_pos;
        bytes20 target;
        (new_pos, target) = bcs_deserialize_offset_bytes20(pos, input);
        uint128 amount;
        (new_pos, amount) = bcs_deserialize_offset_uint128(new_pos, input);
        return (new_pos, BurnEvent(target, amount));
    }

    function bcs_deserialize_BurnEvent(bytes memory input) internal pure returns (BurnEvent memory) {
        uint256 new_pos;
        BurnEvent memory value;
        (new_pos, value) = bcs_deserialize_offset_BurnEvent(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Message {
        uint64 choice;
        // choice=0 corresponds to Credit
        Message_Credit credit;
        // choice=1 corresponds to Withdraw
        Message_Withdraw withdraw;
    }

    function Message_case_credit(Message_Credit memory credit) internal pure returns (Message memory) {
        Message_Withdraw memory withdraw;
        return Message(uint64(0), credit, withdraw);
    }

    function Message_case_withdraw(Message_Withdraw memory withdraw) internal pure returns (Message memory) {
        Message_Credit memory credit;
        return Message(uint64(1), credit, withdraw);
    }

    function bcs_serialize_Message(Message memory input) internal pure returns (bytes memory) {
        if (input.choice == 0) {
            return abi.encodePacked(hex"00", bcs_serialize_Message_Credit(input.credit));
        }
        if (input.choice == 1) {
            return abi.encodePacked(hex"01", bcs_serialize_Message_Withdraw(input.withdraw));
        }
        revert("invalid variant index");
    }

    function bcs_deserialize_offset_Message(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Message memory)
    {
        uint256 new_pos;
        uint256 choice_raw;
        (new_pos, choice_raw) = bcs_deserialize_offset_uleb128(pos, input);
        require(choice_raw <= type(uint64).max, "variant index does not fit in uint64");
        uint64 choice = uint64(choice_raw);
        require(choice < 2, "invalid variant index");
        Message_Credit memory credit;
        if (choice == 0) {
            (new_pos, credit) = bcs_deserialize_offset_Message_Credit(new_pos, input);
        }
        Message_Withdraw memory withdraw;
        if (choice == 1) {
            (new_pos, withdraw) = bcs_deserialize_offset_Message_Withdraw(new_pos, input);
        }
        return (new_pos, Message(choice, credit, withdraw));
    }

    function bcs_deserialize_Message(bytes memory input) internal pure returns (Message memory) {
        uint256 new_pos;
        Message memory value;
        (new_pos, value) = bcs_deserialize_offset_Message(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Message_Credit {
        BridgeTypes.AccountOwner target;
        uint128 amount;
        BridgeTypes.AccountOwner source;
    }

    function bcs_serialize_Message_Credit(Message_Credit memory input) internal pure returns (bytes memory) {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.target);
        result = abi.encodePacked(result, bcs_serialize_uint128(input.amount));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_AccountOwner(input.source));
    }

    function bcs_deserialize_offset_Message_Credit(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Message_Credit memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory target;
        (new_pos, target) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        uint128 amount;
        (new_pos, amount) = bcs_deserialize_offset_uint128(new_pos, input);
        BridgeTypes.AccountOwner memory source;
        (new_pos, source) = BridgeTypes.bcs_deserialize_offset_AccountOwner(new_pos, input);
        return (new_pos, Message_Credit(target, amount, source));
    }

    function bcs_deserialize_Message_Credit(bytes memory input) internal pure returns (Message_Credit memory) {
        uint256 new_pos;
        Message_Credit memory value;
        (new_pos, value) = bcs_deserialize_offset_Message_Credit(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Message_Withdraw {
        BridgeTypes.AccountOwner owner;
        uint128 amount;
        Account target_account;
    }

    function bcs_serialize_Message_Withdraw(Message_Withdraw memory input) internal pure returns (bytes memory) {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, bcs_serialize_uint128(input.amount));
        return abi.encodePacked(result, bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_Message_Withdraw(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Message_Withdraw memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        uint128 amount;
        (new_pos, amount) = bcs_deserialize_offset_uint128(new_pos, input);
        Account memory target_account;
        (new_pos, target_account) = bcs_deserialize_offset_Account(new_pos, input);
        return (new_pos, Message_Withdraw(owner, amount, target_account));
    }

    function bcs_deserialize_Message_Withdraw(bytes memory input) internal pure returns (Message_Withdraw memory) {
        uint256 new_pos;
        Message_Withdraw memory value;
        (new_pos, value) = bcs_deserialize_offset_Message_Withdraw(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation {
        uint64 choice;
        // choice=142627141 corresponds to TickerSymbol
        // choice=144297355 corresponds to Transfer
        WrappedFungibleOperation_Transfer transfer_;
        // choice=158617459 corresponds to Approve
        WrappedFungibleOperation_Approve approve;
        // choice=170732950 corresponds to RegisterAuthorizedCaller
        WrappedFungibleOperation_RegisterAuthorizedCaller register_authorized_caller;
        // choice=198295567 corresponds to MintAndTransfer
        WrappedFungibleOperation_MintAndTransfer mint_and_transfer;
        // choice=204322437 corresponds to Claim
        WrappedFungibleOperation_Claim claim;
        // choice=206964944 corresponds to Balance
        WrappedFungibleOperation_Balance balance_;
        // choice=214048906 corresponds to TransferFrom
        WrappedFungibleOperation_TransferFrom transfer_from;
        // choice=239329758 corresponds to Burn
        WrappedFungibleOperation_Burn burn;
    }

    function WrappedFungibleOperation_case_ticker_symbol() internal pure returns (WrappedFungibleOperation memory) {
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller;
        WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(
            uint64(142627141),
            transfer_,
            approve,
            register_authorized_caller,
            mint_and_transfer,
            claim,
            balance_,
            transfer_from,
            burn
        );
    }

    function WrappedFungibleOperation_case_transfer(WrappedFungibleOperation_Transfer memory transfer_)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller;
        WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(
            uint64(144297355),
            transfer_,
            approve,
            register_authorized_caller,
            mint_and_transfer,
            claim,
            balance_,
            transfer_from,
            burn
        );
    }

    function WrappedFungibleOperation_case_approve(WrappedFungibleOperation_Approve memory approve)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller;
        WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(
            uint64(158617459),
            transfer_,
            approve,
            register_authorized_caller,
            mint_and_transfer,
            claim,
            balance_,
            transfer_from,
            burn
        );
    }

    function WrappedFungibleOperation_case_register_authorized_caller(WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(
            uint64(170732950),
            transfer_,
            approve,
            register_authorized_caller,
            mint_and_transfer,
            claim,
            balance_,
            transfer_from,
            burn
        );
    }

    function WrappedFungibleOperation_case_mint_and_transfer(WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(
            uint64(198295567),
            transfer_,
            approve,
            register_authorized_caller,
            mint_and_transfer,
            claim,
            balance_,
            transfer_from,
            burn
        );
    }

    function WrappedFungibleOperation_case_claim(WrappedFungibleOperation_Claim memory claim)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller;
        WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer;
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(
            uint64(204322437),
            transfer_,
            approve,
            register_authorized_caller,
            mint_and_transfer,
            claim,
            balance_,
            transfer_from,
            burn
        );
    }

    function WrappedFungibleOperation_case_balance(WrappedFungibleOperation_Balance memory balance_)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller;
        WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(
            uint64(206964944),
            transfer_,
            approve,
            register_authorized_caller,
            mint_and_transfer,
            claim,
            balance_,
            transfer_from,
            burn
        );
    }

    function WrappedFungibleOperation_case_transfer_from(WrappedFungibleOperation_TransferFrom memory transfer_from)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller;
        WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(
            uint64(214048906),
            transfer_,
            approve,
            register_authorized_caller,
            mint_and_transfer,
            claim,
            balance_,
            transfer_from,
            burn
        );
    }

    function WrappedFungibleOperation_case_burn(WrappedFungibleOperation_Burn memory burn)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller;
        WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        return WrappedFungibleOperation(
            uint64(239329758),
            transfer_,
            approve,
            register_authorized_caller,
            mint_and_transfer,
            claim,
            balance_,
            transfer_from,
            burn
        );
    }

    function bcs_serialize_WrappedFungibleOperation(WrappedFungibleOperation memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 142627141) {
            return hex"c5a28144";
        }
        if (input.choice == 144297355) {
            return abi.encodePacked(hex"8b9be744", bcs_serialize_WrappedFungibleOperation_Transfer(input.transfer_));
        }
        if (input.choice == 158617459) {
            return abi.encodePacked(hex"f39ed14b", bcs_serialize_WrappedFungibleOperation_Approve(input.approve));
        }
        if (input.choice == 170732950) {
            return abi.encodePacked(
                hex"96dbb451",
                bcs_serialize_WrappedFungibleOperation_RegisterAuthorizedCaller(input.register_authorized_caller)
            );
        }
        if (input.choice == 198295567) {
            return abi.encodePacked(
                hex"8f80c75e", bcs_serialize_WrappedFungibleOperation_MintAndTransfer(input.mint_and_transfer)
            );
        }
        if (input.choice == 204322437) {
            return abi.encodePacked(hex"85edb661", bcs_serialize_WrappedFungibleOperation_Claim(input.claim));
        }
        if (input.choice == 206964944) {
            return abi.encodePacked(hex"d091d862", bcs_serialize_WrappedFungibleOperation_Balance(input.balance_));
        }
        if (input.choice == 214048906) {
            return
                abi.encodePacked(
                    hex"8ac18866", bcs_serialize_WrappedFungibleOperation_TransferFrom(input.transfer_from)
                );
        }
        if (input.choice == 239329758) {
            return abi.encodePacked(hex"dec38f72", bcs_serialize_WrappedFungibleOperation_Burn(input.burn));
        }
        revert("invalid variant index");
    }

    function bcs_deserialize_offset_WrappedFungibleOperation(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation memory)
    {
        uint256 new_pos;
        uint256 choice_raw;
        (new_pos, choice_raw) = bcs_deserialize_offset_uleb128(pos, input);
        require(choice_raw <= type(uint64).max, "variant index does not fit in uint64");
        uint64 choice = uint64(choice_raw);
        require(
            choice == 142627141 || choice == 144297355 || choice == 158617459 || choice == 170732950
                || choice == 198295567 || choice == 204322437 || choice == 206964944 || choice == 214048906
                || choice == 239329758,
            "invalid variant index"
        );
        WrappedFungibleOperation_Transfer memory transfer_;
        if (choice == 144297355) {
            (new_pos, transfer_) = bcs_deserialize_offset_WrappedFungibleOperation_Transfer(new_pos, input);
        }
        WrappedFungibleOperation_Approve memory approve;
        if (choice == 158617459) {
            (new_pos, approve) = bcs_deserialize_offset_WrappedFungibleOperation_Approve(new_pos, input);
        }
        WrappedFungibleOperation_RegisterAuthorizedCaller memory register_authorized_caller;
        if (choice == 170732950) {
            (new_pos, register_authorized_caller) =
                bcs_deserialize_offset_WrappedFungibleOperation_RegisterAuthorizedCaller(new_pos, input);
        }
        WrappedFungibleOperation_MintAndTransfer memory mint_and_transfer;
        if (choice == 198295567) {
            (new_pos, mint_and_transfer) =
                bcs_deserialize_offset_WrappedFungibleOperation_MintAndTransfer(new_pos, input);
        }
        WrappedFungibleOperation_Claim memory claim;
        if (choice == 204322437) {
            (new_pos, claim) = bcs_deserialize_offset_WrappedFungibleOperation_Claim(new_pos, input);
        }
        WrappedFungibleOperation_Balance memory balance_;
        if (choice == 206964944) {
            (new_pos, balance_) = bcs_deserialize_offset_WrappedFungibleOperation_Balance(new_pos, input);
        }
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        if (choice == 214048906) {
            (new_pos, transfer_from) = bcs_deserialize_offset_WrappedFungibleOperation_TransferFrom(new_pos, input);
        }
        WrappedFungibleOperation_Burn memory burn;
        if (choice == 239329758) {
            (new_pos, burn) = bcs_deserialize_offset_WrappedFungibleOperation_Burn(new_pos, input);
        }
        return (
            new_pos,
            WrappedFungibleOperation(
                choice,
                transfer_,
                approve,
                register_authorized_caller,
                mint_and_transfer,
                claim,
                balance_,
                transfer_from,
                burn
            )
        );
    }

    function bcs_deserialize_WrappedFungibleOperation(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation_Approve {
        BridgeTypes.AccountOwner owner;
        BridgeTypes.AccountOwner spender;
        uint128 allowance;
    }

    function bcs_serialize_WrappedFungibleOperation_Approve(WrappedFungibleOperation_Approve memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_AccountOwner(input.spender));
        return abi.encodePacked(result, bcs_serialize_uint128(input.allowance));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_Approve(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_Approve memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        BridgeTypes.AccountOwner memory spender;
        (new_pos, spender) = BridgeTypes.bcs_deserialize_offset_AccountOwner(new_pos, input);
        uint128 allowance;
        (new_pos, allowance) = bcs_deserialize_offset_uint128(new_pos, input);
        return (new_pos, WrappedFungibleOperation_Approve(owner, spender, allowance));
    }

    function bcs_deserialize_WrappedFungibleOperation_Approve(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation_Approve memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation_Approve memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation_Approve(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation_Balance {
        BridgeTypes.AccountOwner owner;
    }

    function bcs_serialize_WrappedFungibleOperation_Balance(WrappedFungibleOperation_Balance memory input)
        internal
        pure
        returns (bytes memory)
    {
        return BridgeTypes.bcs_serialize_AccountOwner(input.owner);
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_Balance(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_Balance memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        return (new_pos, WrappedFungibleOperation_Balance(owner));
    }

    function bcs_deserialize_WrappedFungibleOperation_Balance(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation_Balance memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation_Balance memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation_Balance(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation_Burn {
        BridgeTypes.AccountOwner owner;
        uint128 amount;
    }

    function bcs_serialize_WrappedFungibleOperation_Burn(WrappedFungibleOperation_Burn memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        return abi.encodePacked(result, bcs_serialize_uint128(input.amount));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_Burn(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_Burn memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        uint128 amount;
        (new_pos, amount) = bcs_deserialize_offset_uint128(new_pos, input);
        return (new_pos, WrappedFungibleOperation_Burn(owner, amount));
    }

    function bcs_deserialize_WrappedFungibleOperation_Burn(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation_Burn memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation_Burn memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation_Burn(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation_Claim {
        Account source_account;
        uint128 amount;
        Account target_account;
    }

    function bcs_serialize_WrappedFungibleOperation_Claim(WrappedFungibleOperation_Claim memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_Account(input.source_account);
        result = abi.encodePacked(result, bcs_serialize_uint128(input.amount));
        return abi.encodePacked(result, bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_Claim(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_Claim memory)
    {
        uint256 new_pos;
        Account memory source_account;
        (new_pos, source_account) = bcs_deserialize_offset_Account(pos, input);
        uint128 amount;
        (new_pos, amount) = bcs_deserialize_offset_uint128(new_pos, input);
        Account memory target_account;
        (new_pos, target_account) = bcs_deserialize_offset_Account(new_pos, input);
        return (new_pos, WrappedFungibleOperation_Claim(source_account, amount, target_account));
    }

    function bcs_deserialize_WrappedFungibleOperation_Claim(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation_Claim memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation_Claim memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation_Claim(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation_MintAndTransfer {
        Account target_account;
        uint128 amount;
    }

    function bcs_serialize_WrappedFungibleOperation_MintAndTransfer(WrappedFungibleOperation_MintAndTransfer memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = bcs_serialize_Account(input.target_account);
        return abi.encodePacked(result, bcs_serialize_uint128(input.amount));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_MintAndTransfer(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_MintAndTransfer memory)
    {
        uint256 new_pos;
        Account memory target_account;
        (new_pos, target_account) = bcs_deserialize_offset_Account(pos, input);
        uint128 amount;
        (new_pos, amount) = bcs_deserialize_offset_uint128(new_pos, input);
        return (new_pos, WrappedFungibleOperation_MintAndTransfer(target_account, amount));
    }

    function bcs_deserialize_WrappedFungibleOperation_MintAndTransfer(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation_MintAndTransfer memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation_MintAndTransfer memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation_MintAndTransfer(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation_RegisterAuthorizedCaller {
        ApplicationId app_id;
    }

    function bcs_serialize_WrappedFungibleOperation_RegisterAuthorizedCaller(WrappedFungibleOperation_RegisterAuthorizedCaller memory input)
        internal
        pure
        returns (bytes memory)
    {
        return bcs_serialize_ApplicationId(input.app_id);
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_RegisterAuthorizedCaller(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_RegisterAuthorizedCaller memory)
    {
        uint256 new_pos;
        ApplicationId memory app_id;
        (new_pos, app_id) = bcs_deserialize_offset_ApplicationId(pos, input);
        return (new_pos, WrappedFungibleOperation_RegisterAuthorizedCaller(app_id));
    }

    function bcs_deserialize_WrappedFungibleOperation_RegisterAuthorizedCaller(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation_RegisterAuthorizedCaller memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation_RegisterAuthorizedCaller memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation_RegisterAuthorizedCaller(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation_Transfer {
        BridgeTypes.AccountOwner owner;
        uint128 amount;
        Account target_account;
    }

    function bcs_serialize_WrappedFungibleOperation_Transfer(WrappedFungibleOperation_Transfer memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, bcs_serialize_uint128(input.amount));
        return abi.encodePacked(result, bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_Transfer(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_Transfer memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        uint128 amount;
        (new_pos, amount) = bcs_deserialize_offset_uint128(new_pos, input);
        Account memory target_account;
        (new_pos, target_account) = bcs_deserialize_offset_Account(new_pos, input);
        return (new_pos, WrappedFungibleOperation_Transfer(owner, amount, target_account));
    }

    function bcs_deserialize_WrappedFungibleOperation_Transfer(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation_Transfer memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation_Transfer memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation_Transfer(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation_TransferFrom {
        BridgeTypes.AccountOwner owner;
        BridgeTypes.AccountOwner spender;
        uint128 amount;
        Account target_account;
    }

    function bcs_serialize_WrappedFungibleOperation_TransferFrom(WrappedFungibleOperation_TransferFrom memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_AccountOwner(input.spender));
        result = abi.encodePacked(result, bcs_serialize_uint128(input.amount));
        return abi.encodePacked(result, bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_TransferFrom(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_TransferFrom memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        BridgeTypes.AccountOwner memory spender;
        (new_pos, spender) = BridgeTypes.bcs_deserialize_offset_AccountOwner(new_pos, input);
        uint128 amount;
        (new_pos, amount) = bcs_deserialize_offset_uint128(new_pos, input);
        Account memory target_account;
        (new_pos, target_account) = bcs_deserialize_offset_Account(new_pos, input);
        return (new_pos, WrappedFungibleOperation_TransferFrom(owner, spender, amount, target_account));
    }

    function bcs_deserialize_WrappedFungibleOperation_TransferFrom(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation_TransferFrom memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation_TransferFrom memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation_TransferFrom(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_bytes20(bytes20 input) internal pure returns (bytes memory) {
        return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_bytes20(uint256 pos, bytes memory input) internal pure returns (uint256, bytes20) {
        bytes20 dest;
        assembly ("memory-safe") {
            dest := mload(add(add(input, 0x20), pos))
        }
        return (pos + 20, dest);
    }

    function bcs_serialize_uint128(uint128 input) internal pure returns (bytes memory) {
        bytes memory result = new bytes(16);
        uint128 value = input;
        result[0] = bytes1(uint8(value));
        for (uint256 i = 1; i < 16; i++) {
            value = value >> 8;
            result[i] = bytes1(uint8(value));
        }
        return result;
    }

    function bcs_deserialize_offset_uint128(uint256 pos, bytes memory input) internal pure returns (uint256, uint128) {
        uint128 value = uint8(input[pos + 15]);
        for (uint256 i = 0; i < 15; i++) {
            value = value << 8;
            value += uint8(input[pos + 14 - i]);
        }
        return (pos + 16, value);
    }

    function bcs_deserialize_uint128(bytes memory input) internal pure returns (uint128) {
        uint256 new_pos;
        uint128 value;
        (new_pos, value) = bcs_deserialize_offset_uint128(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }
} // end of library WrappedFungibleTypesV1
