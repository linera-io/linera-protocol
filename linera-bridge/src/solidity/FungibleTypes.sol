/// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;
import "BridgeTypes.sol";

library FungibleTypes {

    struct FungibleOperation {
        uint8 choice;
        // choice=0 corresponds to Balance
        FungibleOperation_Balance balance_;
        // choice=1 corresponds to TickerSymbol
        // choice=2 corresponds to Approve
        FungibleOperation_Approve approve;
        // choice=3 corresponds to Transfer
        FungibleOperation_Transfer transfer_;
        // choice=4 corresponds to TransferFrom
        FungibleOperation_TransferFrom transfer_from;
        // choice=5 corresponds to Claim
        FungibleOperation_Claim claim;
    }

    function FungibleOperation_case_balance(FungibleOperation_Balance memory balance_)
        internal
        pure
        returns (FungibleOperation memory)
    {
        FungibleOperation_Approve memory approve;
        FungibleOperation_Transfer memory transfer_;
        FungibleOperation_TransferFrom memory transfer_from;
        FungibleOperation_Claim memory claim;
        return FungibleOperation(uint8(0), balance_, approve, transfer_, transfer_from, claim);
    }

    function FungibleOperation_case_ticker_symbol()
        internal
        pure
        returns (FungibleOperation memory)
    {
        FungibleOperation_Balance memory balance_;
        FungibleOperation_Approve memory approve;
        FungibleOperation_Transfer memory transfer_;
        FungibleOperation_TransferFrom memory transfer_from;
        FungibleOperation_Claim memory claim;
        return FungibleOperation(uint8(1), balance_, approve, transfer_, transfer_from, claim);
    }

    function FungibleOperation_case_approve(FungibleOperation_Approve memory approve)
        internal
        pure
        returns (FungibleOperation memory)
    {
        FungibleOperation_Balance memory balance_;
        FungibleOperation_Transfer memory transfer_;
        FungibleOperation_TransferFrom memory transfer_from;
        FungibleOperation_Claim memory claim;
        return FungibleOperation(uint8(2), balance_, approve, transfer_, transfer_from, claim);
    }

    function FungibleOperation_case_transfer(FungibleOperation_Transfer memory transfer_)
        internal
        pure
        returns (FungibleOperation memory)
    {
        FungibleOperation_Balance memory balance_;
        FungibleOperation_Approve memory approve;
        FungibleOperation_TransferFrom memory transfer_from;
        FungibleOperation_Claim memory claim;
        return FungibleOperation(uint8(3), balance_, approve, transfer_, transfer_from, claim);
    }

    function FungibleOperation_case_transfer_from(FungibleOperation_TransferFrom memory transfer_from)
        internal
        pure
        returns (FungibleOperation memory)
    {
        FungibleOperation_Balance memory balance_;
        FungibleOperation_Approve memory approve;
        FungibleOperation_Transfer memory transfer_;
        FungibleOperation_Claim memory claim;
        return FungibleOperation(uint8(4), balance_, approve, transfer_, transfer_from, claim);
    }

    function FungibleOperation_case_claim(FungibleOperation_Claim memory claim)
        internal
        pure
        returns (FungibleOperation memory)
    {
        FungibleOperation_Balance memory balance_;
        FungibleOperation_Approve memory approve;
        FungibleOperation_Transfer memory transfer_;
        FungibleOperation_TransferFrom memory transfer_from;
        return FungibleOperation(uint8(5), balance_, approve, transfer_, transfer_from, claim);
    }

    function bcs_serialize_FungibleOperation(FungibleOperation memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_FungibleOperation_Balance(input.balance_));
        }
        if (input.choice == 2) {
            return abi.encodePacked(input.choice, bcs_serialize_FungibleOperation_Approve(input.approve));
        }
        if (input.choice == 3) {
            return abi.encodePacked(input.choice, bcs_serialize_FungibleOperation_Transfer(input.transfer_));
        }
        if (input.choice == 4) {
            return abi.encodePacked(input.choice, bcs_serialize_FungibleOperation_TransferFrom(input.transfer_from));
        }
        if (input.choice == 5) {
            return abi.encodePacked(input.choice, bcs_serialize_FungibleOperation_Claim(input.claim));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_FungibleOperation(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, FungibleOperation memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        FungibleOperation_Balance memory balance_;
        if (choice == 0) {
            (new_pos, balance_) = bcs_deserialize_offset_FungibleOperation_Balance(new_pos, input);
        }
        FungibleOperation_Approve memory approve;
        if (choice == 2) {
            (new_pos, approve) = bcs_deserialize_offset_FungibleOperation_Approve(new_pos, input);
        }
        FungibleOperation_Transfer memory transfer_;
        if (choice == 3) {
            (new_pos, transfer_) = bcs_deserialize_offset_FungibleOperation_Transfer(new_pos, input);
        }
        FungibleOperation_TransferFrom memory transfer_from;
        if (choice == 4) {
            (new_pos, transfer_from) = bcs_deserialize_offset_FungibleOperation_TransferFrom(new_pos, input);
        }
        FungibleOperation_Claim memory claim;
        if (choice == 5) {
            (new_pos, claim) = bcs_deserialize_offset_FungibleOperation_Claim(new_pos, input);
        }
        require(choice < 6);
        return (new_pos, FungibleOperation(choice, balance_, approve, transfer_, transfer_from, claim));
    }

    function bcs_deserialize_FungibleOperation(bytes memory input)
        internal
        pure
        returns (FungibleOperation memory)
    {
        uint256 new_pos;
        FungibleOperation memory value;
        (new_pos, value) = bcs_deserialize_offset_FungibleOperation(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct FungibleOperation_Approve {
        BridgeTypes.AccountOwner owner;
        BridgeTypes.AccountOwner spender;
        BridgeTypes.Amount allowance;
    }

    function bcs_serialize_FungibleOperation_Approve(FungibleOperation_Approve memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_AccountOwner(input.spender));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.allowance));
    }

    function bcs_deserialize_offset_FungibleOperation_Approve(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, FungibleOperation_Approve memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        BridgeTypes.AccountOwner memory spender;
        (new_pos, spender) = BridgeTypes.bcs_deserialize_offset_AccountOwner(new_pos, input);
        BridgeTypes.Amount memory allowance;
        (new_pos, allowance) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        return (new_pos, FungibleOperation_Approve(owner, spender, allowance));
    }

    function bcs_deserialize_FungibleOperation_Approve(bytes memory input)
        internal
        pure
        returns (FungibleOperation_Approve memory)
    {
        uint256 new_pos;
        FungibleOperation_Approve memory value;
        (new_pos, value) = bcs_deserialize_offset_FungibleOperation_Approve(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct FungibleOperation_Balance {
        BridgeTypes.AccountOwner owner;
    }

    function bcs_serialize_FungibleOperation_Balance(FungibleOperation_Balance memory input)
        internal
        pure
        returns (bytes memory)
    {
        return BridgeTypes.bcs_serialize_AccountOwner(input.owner);
    }

    function bcs_deserialize_offset_FungibleOperation_Balance(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, FungibleOperation_Balance memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        return (new_pos, FungibleOperation_Balance(owner));
    }

    function bcs_deserialize_FungibleOperation_Balance(bytes memory input)
        internal
        pure
        returns (FungibleOperation_Balance memory)
    {
        uint256 new_pos;
        FungibleOperation_Balance memory value;
        (new_pos, value) = bcs_deserialize_offset_FungibleOperation_Balance(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct FungibleOperation_Claim {
        BridgeTypes.Account source_account;
        BridgeTypes.Amount amount;
        BridgeTypes.Account target_account;
    }

    function bcs_serialize_FungibleOperation_Claim(FungibleOperation_Claim memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_Account(input.source_account);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_FungibleOperation_Claim(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, FungibleOperation_Claim memory)
    {
        uint256 new_pos;
        BridgeTypes.Account memory source_account;
        (new_pos, source_account) = BridgeTypes.bcs_deserialize_offset_Account(pos, input);
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        BridgeTypes.Account memory target_account;
        (new_pos, target_account) = BridgeTypes.bcs_deserialize_offset_Account(new_pos, input);
        return (new_pos, FungibleOperation_Claim(source_account, amount, target_account));
    }

    function bcs_deserialize_FungibleOperation_Claim(bytes memory input)
        internal
        pure
        returns (FungibleOperation_Claim memory)
    {
        uint256 new_pos;
        FungibleOperation_Claim memory value;
        (new_pos, value) = bcs_deserialize_offset_FungibleOperation_Claim(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct FungibleOperation_Transfer {
        BridgeTypes.AccountOwner owner;
        BridgeTypes.Amount amount;
        BridgeTypes.Account target_account;
    }

    function bcs_serialize_FungibleOperation_Transfer(FungibleOperation_Transfer memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_FungibleOperation_Transfer(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, FungibleOperation_Transfer memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        BridgeTypes.Account memory target_account;
        (new_pos, target_account) = BridgeTypes.bcs_deserialize_offset_Account(new_pos, input);
        return (new_pos, FungibleOperation_Transfer(owner, amount, target_account));
    }

    function bcs_deserialize_FungibleOperation_Transfer(bytes memory input)
        internal
        pure
        returns (FungibleOperation_Transfer memory)
    {
        uint256 new_pos;
        FungibleOperation_Transfer memory value;
        (new_pos, value) = bcs_deserialize_offset_FungibleOperation_Transfer(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct FungibleOperation_TransferFrom {
        BridgeTypes.AccountOwner owner;
        BridgeTypes.AccountOwner spender;
        BridgeTypes.Amount amount;
        BridgeTypes.Account target_account;
    }

    function bcs_serialize_FungibleOperation_TransferFrom(FungibleOperation_TransferFrom memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_AccountOwner(input.spender));
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_FungibleOperation_TransferFrom(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, FungibleOperation_TransferFrom memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        BridgeTypes.AccountOwner memory spender;
        (new_pos, spender) = BridgeTypes.bcs_deserialize_offset_AccountOwner(new_pos, input);
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        BridgeTypes.Account memory target_account;
        (new_pos, target_account) = BridgeTypes.bcs_deserialize_offset_Account(new_pos, input);
        return (new_pos, FungibleOperation_TransferFrom(owner, spender, amount, target_account));
    }

    function bcs_deserialize_FungibleOperation_TransferFrom(bytes memory input)
        internal
        pure
        returns (FungibleOperation_TransferFrom memory)
    {
        uint256 new_pos;
        FungibleOperation_TransferFrom memory value;
        (new_pos, value) = bcs_deserialize_offset_FungibleOperation_TransferFrom(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Message {
        uint8 choice;
        // choice=0 corresponds to Credit
        Message_Credit credit;
        // choice=1 corresponds to Withdraw
        Message_Withdraw withdraw;
    }

    function Message_case_credit(Message_Credit memory credit)
        internal
        pure
        returns (Message memory)
    {
        Message_Withdraw memory withdraw;
        return Message(uint8(0), credit, withdraw);
    }

    function Message_case_withdraw(Message_Withdraw memory withdraw)
        internal
        pure
        returns (Message memory)
    {
        Message_Credit memory credit;
        return Message(uint8(1), credit, withdraw);
    }

    function bcs_serialize_Message(Message memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_Message_Credit(input.credit));
        }
        if (input.choice == 1) {
            return abi.encodePacked(input.choice, bcs_serialize_Message_Withdraw(input.withdraw));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_Message(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Message memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        Message_Credit memory credit;
        if (choice == 0) {
            (new_pos, credit) = bcs_deserialize_offset_Message_Credit(new_pos, input);
        }
        Message_Withdraw memory withdraw;
        if (choice == 1) {
            (new_pos, withdraw) = bcs_deserialize_offset_Message_Withdraw(new_pos, input);
        }
        require(choice < 2);
        return (new_pos, Message(choice, credit, withdraw));
    }

    function bcs_deserialize_Message(bytes memory input)
        internal
        pure
        returns (Message memory)
    {
        uint256 new_pos;
        Message memory value;
        (new_pos, value) = bcs_deserialize_offset_Message(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Message_Credit {
        BridgeTypes.AccountOwner target;
        BridgeTypes.Amount amount;
        BridgeTypes.AccountOwner source;
    }

    function bcs_serialize_Message_Credit(Message_Credit memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.target);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
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
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        BridgeTypes.AccountOwner memory source;
        (new_pos, source) = BridgeTypes.bcs_deserialize_offset_AccountOwner(new_pos, input);
        return (new_pos, Message_Credit(target, amount, source));
    }

    function bcs_deserialize_Message_Credit(bytes memory input)
        internal
        pure
        returns (Message_Credit memory)
    {
        uint256 new_pos;
        Message_Credit memory value;
        (new_pos, value) = bcs_deserialize_offset_Message_Credit(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct Message_Withdraw {
        BridgeTypes.AccountOwner owner;
        BridgeTypes.Amount amount;
        BridgeTypes.Account target_account;
    }

    function bcs_serialize_Message_Withdraw(Message_Withdraw memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_Message_Withdraw(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, Message_Withdraw memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        BridgeTypes.Account memory target_account;
        (new_pos, target_account) = BridgeTypes.bcs_deserialize_offset_Account(new_pos, input);
        return (new_pos, Message_Withdraw(owner, amount, target_account));
    }

    function bcs_deserialize_Message_Withdraw(bytes memory input)
        internal
        pure
        returns (Message_Withdraw memory)
    {
        uint256 new_pos;
        Message_Withdraw memory value;
        (new_pos, value) = bcs_deserialize_offset_Message_Withdraw(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    function bcs_serialize_uint8(uint8 input)
        internal
        pure
        returns (bytes memory)
    {
      return abi.encodePacked(input);
    }

    function bcs_deserialize_offset_uint8(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, uint8)
    {
        uint8 value = uint8(input[pos]);
        return (pos + 1, value);
    }

    function bcs_deserialize_uint8(bytes memory input)
        internal
        pure
        returns (uint8)
    {
        uint256 new_pos;
        uint8 value;
        (new_pos, value) = bcs_deserialize_offset_uint8(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

} // end of library FungibleTypes
