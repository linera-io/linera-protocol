// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract ExampleCallWasmCounter {
  bytes32 universal_address;
  constructor(bytes32 _universal_address) {
    universal_address = _universal_address;
  }


  struct CounterOperation {
    uint8 choice;
    // choice=0 corresponds to Increment
    uint64 increment;
  }
  function bcs_serialize_CounterOperation(CounterOperation memory input) internal pure returns (bytes memory) {
    bytes memory result = abi.encodePacked(input.choice);
    if (input.choice == 0) {
      return abi.encodePacked(result, bcs_serialize_uint64(input.increment));
    }
    return result;
  }


  struct CounterRequest {
    uint8 choice;
    // choice=0 corresponds to Query
    // choice=1 corresponds to Increment
    uint64 increment;
  }


  function bcs_serialize_uint64(uint64 input) internal pure returns (bytes memory) {
    bytes memory result = new bytes(8);
    uint64 value = input;
    result[0] = bytes1(uint8(value));
    for (uint i=1; i<8; i++) {
      value = value >> 8;
      result[i] = bytes1(uint8(value));
    }
    return result;
  }
  function bcs_deserialize_offset_uint64(uint256 pos, bytes memory input) internal pure returns (uint256, uint64) {
    require(pos + 7 < input.length, "Position out of bound");
    uint64 value = uint8(input[pos + 7]);
    for (uint256 i=0; i<7; i++) {
      value = value << 8;
      value += uint8(input[pos + 6 - i]);
    }
    return (pos + 8, value);
  }

  function bcs_deserialize_offset_uint8(uint256 pos, bytes memory input) internal pure returns (uint256, uint8) {
    require(pos < input.length, "Position out of bound");
    uint8 value = uint8(input[pos]);
    return (pos + 1, value);
  }


  function bcs_deserialize_uint64(bytes memory input) public pure returns (uint64) {
    uint256 new_pos;
    uint64 value;
    (new_pos, value) = bcs_deserialize_offset_uint64(0, input);
    require(new_pos == input.length, "incomplete deserialization");
    return value;
  }

  function serde_json_serialize_uint64(uint64 input) public pure returns (bytes memory) {
    if (input < 10) {
      bytes memory data = abi.encodePacked(uint8(48 + input));
      return data;
    }
    if (input < 100) {
      uint64 val1 = input % 10;
      uint64 val2 = (input - val1) / 10;
      bytes memory data = abi.encodePacked(uint8(48 + val2), uint8(48 + val1));
      return data;
    }
    require(false);
  }


  function serde_json_serialize_CounterRequest(CounterRequest memory input) public pure returns (bytes memory) {
    if (input.choice == 0) {
      bytes memory data = abi.encodePacked(uint8(34), uint8(81), uint8(117), uint8(101), uint8(114), uint8(121), uint8(34));
      return data;
    }
    bytes memory blk1 = abi.encodePacked(uint8(123), uint8(34), uint8(73), uint8(110), uint8(99), uint8(114), uint8(101), uint8(109), uint8(101), uint8(110), uint8(116), uint8(34), uint8(58));
    bytes memory blk2 = serde_json_serialize_uint64(input.increment);
    bytes memory blk3 = abi.encodePacked(uint8(125));
    return abi.encodePacked(blk1, blk2, blk3);
  }

  function serde_json_deserialize_u64(bytes memory input) public pure returns (uint64) {
    uint64 value = 0;
    uint256 len = input.length;
    uint64 pow = 1;
    for (uint256 idx=0; idx<len; idx++) {
      uint256 jdx = len - 1 - idx;
      uint8 val_idx = uint8(input[jdx]) - 48;
      value = value + uint64(val_idx) * pow;
      pow = pow * 10;
    }
    return value;
  }


  function nest_increment(uint64 input1) external returns (uint64) {
    address precompile = address(0x0b);
    CounterOperation memory input2 = CounterOperation({choice: 0, increment: input1});
    bytes memory input3 = bcs_serialize_CounterOperation(input2);
    bytes memory input4 = abi.encodePacked(universal_address, input3);
    (bool success, bytes memory return1) = precompile.call(input4);
    require(success);
    uint64 return2 = bcs_deserialize_uint64(return1);
    return return2;
  }

  function nest_get_value() external returns (uint64) {
    address precompile = address(0x0b);
    CounterRequest memory input2 = CounterRequest({choice:0, increment: 0});
    bytes memory input3 = serde_json_serialize_CounterRequest(input2);
    bytes memory input4 = abi.encodePacked(universal_address, input3);
    (bool success, bytes memory return1) = precompile.call(input4);
    require(success);
    uint64 return2 = serde_json_deserialize_u64(return1);
    return return2;
  }

}
