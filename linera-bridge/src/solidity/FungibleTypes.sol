/// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.0;
import "BridgeTypes.sol";

library FungibleTypes {

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

    struct WrappedFungibleOperation {
        uint8 choice;
        // choice=0 corresponds to Balance
        WrappedFungibleOperation_Balance balance_;
        // choice=1 corresponds to TickerSymbol
        // choice=2 corresponds to Approve
        WrappedFungibleOperation_Approve approve;
        // choice=3 corresponds to Transfer
        WrappedFungibleOperation_Transfer transfer_;
        // choice=4 corresponds to TransferFrom
        WrappedFungibleOperation_TransferFrom transfer_from;
        // choice=5 corresponds to Claim
        WrappedFungibleOperation_Claim claim;
        // choice=6 corresponds to Mint
        WrappedFungibleOperation_Mint mint;
        // choice=7 corresponds to Burn
        WrappedFungibleOperation_Burn burn;
    }

    function WrappedFungibleOperation_case_balance(WrappedFungibleOperation_Balance memory balance_)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Mint memory mint;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(uint8(0), balance_, approve, transfer_, transfer_from, claim, mint, burn);
    }

    function WrappedFungibleOperation_case_ticker_symbol()
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Mint memory mint;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(uint8(1), balance_, approve, transfer_, transfer_from, claim, mint, burn);
    }

    function WrappedFungibleOperation_case_approve(WrappedFungibleOperation_Approve memory approve)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Mint memory mint;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(uint8(2), balance_, approve, transfer_, transfer_from, claim, mint, burn);
    }

    function WrappedFungibleOperation_case_transfer(WrappedFungibleOperation_Transfer memory transfer_)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Mint memory mint;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(uint8(3), balance_, approve, transfer_, transfer_from, claim, mint, burn);
    }

    function WrappedFungibleOperation_case_transfer_from(WrappedFungibleOperation_TransferFrom memory transfer_from)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Mint memory mint;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(uint8(4), balance_, approve, transfer_, transfer_from, claim, mint, burn);
    }

    function WrappedFungibleOperation_case_claim(WrappedFungibleOperation_Claim memory claim)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Mint memory mint;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(uint8(5), balance_, approve, transfer_, transfer_from, claim, mint, burn);
    }

    function WrappedFungibleOperation_case_mint(WrappedFungibleOperation_Mint memory mint)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Burn memory burn;
        return WrappedFungibleOperation(uint8(6), balance_, approve, transfer_, transfer_from, claim, mint, burn);
    }

    function WrappedFungibleOperation_case_burn(WrappedFungibleOperation_Burn memory burn)
        internal
        pure
        returns (WrappedFungibleOperation memory)
    {
        WrappedFungibleOperation_Balance memory balance_;
        WrappedFungibleOperation_Approve memory approve;
        WrappedFungibleOperation_Transfer memory transfer_;
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        WrappedFungibleOperation_Claim memory claim;
        WrappedFungibleOperation_Mint memory mint;
        return WrappedFungibleOperation(uint8(7), balance_, approve, transfer_, transfer_from, claim, mint, burn);
    }

    function bcs_serialize_WrappedFungibleOperation(WrappedFungibleOperation memory input)
        internal
        pure
        returns (bytes memory)
    {
        if (input.choice == 0) {
            return abi.encodePacked(input.choice, bcs_serialize_WrappedFungibleOperation_Balance(input.balance_));
        }
        if (input.choice == 2) {
            return abi.encodePacked(input.choice, bcs_serialize_WrappedFungibleOperation_Approve(input.approve));
        }
        if (input.choice == 3) {
            return abi.encodePacked(input.choice, bcs_serialize_WrappedFungibleOperation_Transfer(input.transfer_));
        }
        if (input.choice == 4) {
            return abi.encodePacked(input.choice, bcs_serialize_WrappedFungibleOperation_TransferFrom(input.transfer_from));
        }
        if (input.choice == 5) {
            return abi.encodePacked(input.choice, bcs_serialize_WrappedFungibleOperation_Claim(input.claim));
        }
        if (input.choice == 6) {
            return abi.encodePacked(input.choice, bcs_serialize_WrappedFungibleOperation_Mint(input.mint));
        }
        if (input.choice == 7) {
            return abi.encodePacked(input.choice, bcs_serialize_WrappedFungibleOperation_Burn(input.burn));
        }
        return abi.encodePacked(input.choice);
    }

    function bcs_deserialize_offset_WrappedFungibleOperation(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation memory)
    {
        uint256 new_pos;
        uint8 choice;
        (new_pos, choice) = bcs_deserialize_offset_uint8(pos, input);
        WrappedFungibleOperation_Balance memory balance_;
        if (choice == 0) {
            (new_pos, balance_) = bcs_deserialize_offset_WrappedFungibleOperation_Balance(new_pos, input);
        }
        WrappedFungibleOperation_Approve memory approve;
        if (choice == 2) {
            (new_pos, approve) = bcs_deserialize_offset_WrappedFungibleOperation_Approve(new_pos, input);
        }
        WrappedFungibleOperation_Transfer memory transfer_;
        if (choice == 3) {
            (new_pos, transfer_) = bcs_deserialize_offset_WrappedFungibleOperation_Transfer(new_pos, input);
        }
        WrappedFungibleOperation_TransferFrom memory transfer_from;
        if (choice == 4) {
            (new_pos, transfer_from) = bcs_deserialize_offset_WrappedFungibleOperation_TransferFrom(new_pos, input);
        }
        WrappedFungibleOperation_Claim memory claim;
        if (choice == 5) {
            (new_pos, claim) = bcs_deserialize_offset_WrappedFungibleOperation_Claim(new_pos, input);
        }
        WrappedFungibleOperation_Mint memory mint;
        if (choice == 6) {
            (new_pos, mint) = bcs_deserialize_offset_WrappedFungibleOperation_Mint(new_pos, input);
        }
        WrappedFungibleOperation_Burn memory burn;
        if (choice == 7) {
            (new_pos, burn) = bcs_deserialize_offset_WrappedFungibleOperation_Burn(new_pos, input);
        }
        require(choice < 8);
        return (new_pos, WrappedFungibleOperation(choice, balance_, approve, transfer_, transfer_from, claim, mint, burn));
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
        BridgeTypes.Amount allowance;
    }

    function bcs_serialize_WrappedFungibleOperation_Approve(WrappedFungibleOperation_Approve memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_AccountOwner(input.spender));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.allowance));
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
        BridgeTypes.Amount memory allowance;
        (new_pos, allowance) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
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
        BridgeTypes.Amount amount;
    }

    function bcs_serialize_WrappedFungibleOperation_Burn(WrappedFungibleOperation_Burn memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_Burn(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_Burn memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
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
        BridgeTypes.Account source_account;
        BridgeTypes.Amount amount;
        BridgeTypes.Account target_account;
    }

    function bcs_serialize_WrappedFungibleOperation_Claim(WrappedFungibleOperation_Claim memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_Account(input.source_account);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_Claim(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_Claim memory)
    {
        uint256 new_pos;
        BridgeTypes.Account memory source_account;
        (new_pos, source_account) = BridgeTypes.bcs_deserialize_offset_Account(pos, input);
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        BridgeTypes.Account memory target_account;
        (new_pos, target_account) = BridgeTypes.bcs_deserialize_offset_Account(new_pos, input);
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

    struct WrappedFungibleOperation_Mint {
        BridgeTypes.Account target_account;
        BridgeTypes.Amount amount;
    }

    function bcs_serialize_WrappedFungibleOperation_Mint(WrappedFungibleOperation_Mint memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_Account(input.target_account);
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_Mint(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_Mint memory)
    {
        uint256 new_pos;
        BridgeTypes.Account memory target_account;
        (new_pos, target_account) = BridgeTypes.bcs_deserialize_offset_Account(pos, input);
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        return (new_pos, WrappedFungibleOperation_Mint(target_account, amount));
    }

    function bcs_deserialize_WrappedFungibleOperation_Mint(bytes memory input)
        internal
        pure
        returns (WrappedFungibleOperation_Mint memory)
    {
        uint256 new_pos;
        WrappedFungibleOperation_Mint memory value;
        (new_pos, value) = bcs_deserialize_offset_WrappedFungibleOperation_Mint(0, input);
        require(new_pos == input.length, "incomplete deserialization");
        return value;
    }

    struct WrappedFungibleOperation_Transfer {
        BridgeTypes.AccountOwner owner;
        BridgeTypes.Amount amount;
        BridgeTypes.Account target_account;
    }

    function bcs_serialize_WrappedFungibleOperation_Transfer(WrappedFungibleOperation_Transfer memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Account(input.target_account));
    }

    function bcs_deserialize_offset_WrappedFungibleOperation_Transfer(uint256 pos, bytes memory input)
        internal
        pure
        returns (uint256, WrappedFungibleOperation_Transfer memory)
    {
        uint256 new_pos;
        BridgeTypes.AccountOwner memory owner;
        (new_pos, owner) = BridgeTypes.bcs_deserialize_offset_AccountOwner(pos, input);
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        BridgeTypes.Account memory target_account;
        (new_pos, target_account) = BridgeTypes.bcs_deserialize_offset_Account(new_pos, input);
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
        BridgeTypes.Amount amount;
        BridgeTypes.Account target_account;
    }

    function bcs_serialize_WrappedFungibleOperation_TransferFrom(WrappedFungibleOperation_TransferFrom memory input)
        internal
        pure
        returns (bytes memory)
    {
        bytes memory result = BridgeTypes.bcs_serialize_AccountOwner(input.owner);
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_AccountOwner(input.spender));
        result = abi.encodePacked(result, BridgeTypes.bcs_serialize_Amount(input.amount));
        return abi.encodePacked(result, BridgeTypes.bcs_serialize_Account(input.target_account));
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
        BridgeTypes.Amount memory amount;
        (new_pos, amount) = BridgeTypes.bcs_deserialize_offset_Amount(new_pos, input);
        BridgeTypes.Account memory target_account;
        (new_pos, target_account) = BridgeTypes.bcs_deserialize_offset_Account(new_pos, input);
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
