// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Sanitizer of WebAssembly bytecodes to ensure they will consume the same amount of fuel in both
//! the [Wasmer](https://wasmer.io) and [Wasmtime](https://wasmtime.dev) runtimes.
//!
//! Ensures that all functions in the module end with a `return` instruction, because when a
//! function finishes without such an instruction in Wasmer no fuel is consumed, while in Wasmtime,
//! one unit of fuel is consumed.

use crate::Bytecode;
use std::{borrow::Cow, mem, ops::Range};
use wasm_encoder::{Encode, Function};
use wasmparser::{BinaryReaderError, FunctionBody, Parser, Payload};

/// Sanitizes the input `bytecode`, ensuring that all functions end with a `return` instruction.
///
/// Returns the sanitized bytecode.
pub fn sanitize(bytecode: Bytecode) -> Result<Bytecode, anyhow::Error> {
    let changed_bytecode = match Sanitizer::new(bytecode.as_ref()).sanitize()? {
        Cow::Borrowed(_) => None,
        Cow::Owned(changed_bytecode) => Some(Bytecode::new(changed_bytecode)),
    };

    Ok(changed_bytecode.unwrap_or(bytecode))
}

/// Sanitizer of WebAssembly bytecodes.
///
/// Ensures all functions end with a `return` instruction. See the module level documentation for
/// more information.
pub struct Sanitizer<'bytecode> {
    bytecode: &'bytecode [u8],
    parsed_items:
        Box<dyn Iterator<Item = Result<Payload<'bytecode>, BinaryReaderError>> + 'bytecode>,
    current_code_section: Range<usize>,
    queued_functions: Vec<FunctionBody<'bytecode>>,
    output: Option<Vec<u8>>,
    completed_offset: usize,
}

impl<'bytecode> Sanitizer<'bytecode> {
    /// Creates a new [`Sanitizer`] for the WebAssembly `bytecode`.
    pub fn new(bytecode: &'bytecode [u8]) -> Self {
        Sanitizer {
            bytecode,
            parsed_items: Box::new(Parser::default().parse_all(bytecode)),
            current_code_section: 0..0,
            queued_functions: Vec::new(),
            output: None,
            completed_offset: 0,
        }
    }

    /// Sanitizes the input `bytecode`, and returns the sanitized bytecode.
    ///
    /// Ensures that all functions in the `bytecode` end with a `return` instruction.
    pub fn sanitize(&mut self) -> Result<Cow<'bytecode, [u8]>, BinaryReaderError> {
        loop {
            match self.parsed_items.next().transpose()? {
                Some(Payload::CodeSectionStart { range, size, .. }) => {
                    let size_prefix_length = match size {
                        0x00..=0x7f => 1,
                        0x80..=0x3fff => 2,
                        0x4000..=0x1f_ffff => 3,
                        0x20_0000..=0x0fff_ffff => 4,
                        0x1000_0000.. => 5,
                    };
                    self.current_code_section = range;
                    self.current_code_section.start -= size_prefix_length;
                    self.queued_functions.clear();
                }
                Some(Payload::CodeSectionEntry(function_body)) => {
                    self.queued_functions.push(function_body);
                }
                Some(_) => self.maybe_sanitize_code_section()?,
                None => {
                    self.maybe_sanitize_code_section()?;

                    let remaining_bytes = &self.bytecode[self.completed_offset..];

                    return if let Some(mut output) = self.output.take() {
                        output.extend(remaining_bytes);
                        Ok(Cow::Owned(output))
                    } else {
                        Ok(Cow::Borrowed(self.bytecode))
                    };
                }
            }
        }
    }

    /// Sanitizes all the functions parsed from the module's Code section if any of them need to be
    /// sanitized.
    fn maybe_sanitize_code_section(&mut self) -> Result<(), BinaryReaderError> {
        if !self.queued_functions.is_empty() {
            if self
                .queued_functions
                .iter()
                .any(Self::function_needs_to_be_sanitized)
            {
                self.sanitize_code_section()?;
            } else {
                self.queued_functions.clear();
            }
        }

        Ok(())
    }

    /// Checks if a function needs to be sanitized.
    ///
    /// Returns `false` if the function is valid and ends with a `return` operation, which means it
    /// is already sanitized.
    fn function_needs_to_be_sanitized(function: &FunctionBody<'bytecode>) -> bool {
        let operators = match function.get_operators_reader() {
            Ok(operators) => operators,
            Err(_) => return true,
        };

        let two_last_operations = operators
            .into_iter()
            .try_fold((None, None), |(_before_last, last), current| {
                current.map(|current| (last, Some(current)))
            });

        !matches!(
            two_last_operations,
            Ok((
                Some(wasmparser::Operator::Return),
                Some(wasmparser::Operator::End)
            ))
        )
    }

    /// Sanitizes all the functions parsed from the module's Code section.
    fn sanitize_code_section(&mut self) -> Result<(), BinaryReaderError> {
        let mut section = wasm_encoder::CodeSection::new();

        for function in mem::take(&mut self.queued_functions) {
            section.function(&Self::sanitize_function(function)?);
        }

        self.copy_until(self.current_code_section.start);
        section.encode(self.output());
        self.completed_offset = self.current_code_section.end;

        Ok(())
    }

    /// Sanitizes a parsed function that is known to not have an `return` instruction at the end.
    fn sanitize_function(function: FunctionBody<'_>) -> Result<Function, BinaryReaderError> {
        let locals = Self::convert_locals(function.get_locals_reader()?)?;
        let mut sanitized_function = Function::new(locals);
        let mut instructions = Self::convert_operators(function.get_operators_reader()?)?;
        let last_instruction = instructions.pop();
        let instruction_before_last = instructions.pop();

        for instruction in instructions {
            sanitized_function.instruction(&instruction);
        }

        match (instruction_before_last, last_instruction) {
            (Some(wasm_encoder::Instruction::Return), Some(wasm_encoder::Instruction::End))
            | (None, Some(wasm_encoder::Instruction::End | wasm_encoder::Instruction::Return))
            | (None, None) => {
                sanitized_function.instruction(&wasm_encoder::Instruction::Return);
                sanitized_function.instruction(&wasm_encoder::Instruction::End);
            }
            (
                Some(last),
                Some(wasm_encoder::Instruction::End | wasm_encoder::Instruction::Return),
            )
            | (None, Some(last)) => {
                sanitized_function.instruction(&last);
                sanitized_function.instruction(&wasm_encoder::Instruction::Return);
                sanitized_function.instruction(&wasm_encoder::Instruction::End);
            }
            (Some(before_last), Some(last)) => {
                sanitized_function.instruction(&before_last);
                sanitized_function.instruction(&last);
                sanitized_function.instruction(&wasm_encoder::Instruction::Return);
                sanitized_function.instruction(&wasm_encoder::Instruction::End);
            }
            (Some(_), None) => unreachable!("Right is popped off the vector first"),
        }

        Ok(sanitized_function)
    }

    /// Copies bytes from the input `bytecode` to the `output`, starting at the `completed_offset`
    /// and ending at the `end_offset`.
    fn copy_until(&mut self, end_offset: usize) {
        let start_offset = self.completed_offset;
        let bytes = &self.bytecode[start_offset..end_offset];

        self.output().extend(bytes);
        self.completed_offset = end_offset;
    }

    /// Retrieves the mutable reference to the `output` bytecode vector.
    fn output(&mut self) -> &mut Vec<u8> {
        self.output.get_or_insert_with(Vec::new)
    }

    /// Converts function locals parsed by [`wasmparser`] into locals encodable with
    /// [`wasm-encoder`].
    fn convert_locals(
        locals: impl IntoIterator<Item = Result<(u32, wasmparser::ValType), BinaryReaderError>>,
    ) -> Result<Vec<(u32, wasm_encoder::ValType)>, BinaryReaderError> {
        locals
            .into_iter()
            .map(|maybe_local| {
                maybe_local.map(|(index, parsed_type)| (index, Self::convert_type(parsed_type)))
            })
            .collect()
    }

    /// Converts a WebAssembly type parsed by [`wasmparser`] into a type encodable with
    /// [`wasm_encoder`].
    fn convert_type(parsed_type: wasmparser::ValType) -> wasm_encoder::ValType {
        match parsed_type {
            wasmparser::ValType::I32 => wasm_encoder::ValType::I32,
            wasmparser::ValType::I64 => wasm_encoder::ValType::I64,
            wasmparser::ValType::F32 => wasm_encoder::ValType::F32,
            wasmparser::ValType::F64 => wasm_encoder::ValType::F64,
            wasmparser::ValType::V128 => wasm_encoder::ValType::V128,
            wasmparser::ValType::Ref(wasmparser::RefType {
                nullable,
                heap_type,
            }) => wasm_encoder::ValType::Ref(wasm_encoder::RefType {
                nullable,
                heap_type: Self::convert_heap_type(heap_type),
            }),
        }
    }

    /// Converts a WebAssembly heap type parsed by [`wasmparser`] into a heap type encodable with
    /// [`wasm_encoder`].
    fn convert_heap_type(parsed_type: wasmparser::HeapType) -> wasm_encoder::HeapType {
        match parsed_type {
            wasmparser::HeapType::Func => wasm_encoder::HeapType::Func,
            wasmparser::HeapType::Extern => wasm_encoder::HeapType::Extern,
            wasmparser::HeapType::TypedFunc(function_index) => {
                wasm_encoder::HeapType::TypedFunc(function_index.into())
            }
        }
    }

    /// Converts WebAssembly instructions parsed by [`wasmparser`] into instructions encodable with
    /// [`wasm-encoder`].
    fn convert_operators<'op>(
        operators: impl IntoIterator<Item = Result<wasmparser::Operator<'op>, BinaryReaderError>>,
    ) -> Result<Vec<wasm_encoder::Instruction<'op>>, BinaryReaderError> {
        operators
            .into_iter()
            .map(|maybe_operator| maybe_operator.and_then(Self::convert_operator))
            .collect()
    }

    /// Converts a WebAssembly instruction parsed by [`wasmparser`] into an instruction encodable
    /// with [`wasm-encoder`].
    fn convert_operator(
        operator: wasmparser::Operator<'_>,
    ) -> Result<wasm_encoder::Instruction<'_>, BinaryReaderError> {
        use wasm_encoder::Instruction;
        use wasmparser::Operator;

        macro_rules! convert {
            (
                $( @$proposal:ident $op:ident $({ $($arg:ident: $argty:ty),* })? => $visit:ident)*
            ) => {
                #[allow(unused_variables)]
                Ok(match operator {
                    $(
                        Operator::$op $({ $($arg),* })? => {
                            convert!($op $( $( $arg $arg ),* )?)
                        }
                    )*
                })
            };

            ($op:ident) => { Instruction::$op };
            ($op:ident $tag_index:ident tag_index) => { Instruction::$op($tag_index) };
            ($op:ident $local_index:ident local_index) => { Instruction::$op($local_index) };
            ($op:ident $global_index:ident global_index) => { Instruction::$op($global_index) };
            ($op:ident $data_index:ident data_index) => { Instruction::$op($data_index) };
            ($op:ident $elem_index:ident elem_index) => { Instruction::$op($elem_index) };
            ($op:ident $mem:ident mem) => { Instruction::$op($mem) };
            ($op:ident $table:ident table) => { Instruction::$op($table) };
            ($op:ident $lane:ident lane) => { Instruction::$op($lane) };
            ($op:ident $lanes:ident lanes) => { Instruction::$op($lanes) };
            ($op:ident $relative_depth:ident relative_depth) => {
                Instruction::$op($relative_depth)
            };
            ($op:ident $function_index:ident function_index) => {
                Instruction::$op($function_index.into())
            };

            ($op:ident $blockty:ident blockty) => {{
                let block_type = match $blockty {
                    wasmparser::BlockType::Empty => wasm_encoder::BlockType::Empty,
                    wasmparser::BlockType::Type(val_type) => {
                        wasm_encoder::BlockType::Result(Self::convert_type(val_type))
                    }
                    wasmparser::BlockType::FuncType(function_index) => {
                        wasm_encoder::BlockType::FunctionType(function_index)
                    }
                };
                Instruction::$op(block_type)
            }};

            ($op:ident $memarg:ident memarg) => {
                Instruction::$op(wasm_encoder::MemArg {
                    offset: $memarg.offset,
                    align: $memarg.align.into(),
                    memory_index: $memarg.memory,
                })
            };

            ($op:ident $hty:ident hty) => { Instruction::$op(Self::convert_heap_type($hty)) };
            ($op:ident $mem:ident mem, $mem_byte:ident mem_byte) => { Instruction::$op($mem) };

            ($op:ident $memarg:ident memarg, $lane:ident lane) => {
                Instruction::$op {
                    memarg: wasm_encoder::MemArg {
                        offset: $memarg.offset,
                        align: $memarg.align.into(),
                        memory_index: $memarg.memory,
                    },
                    lane: $lane,
                }
            };

            (I32Const $value:ident value) => { Instruction::I32Const($value) };
            (I64Const $value:ident value) => { Instruction::I64Const($value) };
            (V128Const $value:ident value) => { Instruction::V128Const($value.i128()) };
            (F32Const $value:ident value) => {
                Instruction::F32Const(f32::from_bits($value.bits()))
            };
            (F64Const $value:ident value) => {
                Instruction::F64Const(f64::from_bits($value.bits()))
            };

            (BrTable $targets:ident targets) => { Instruction::BrTable(
                $targets.targets().collect::<Result<Vec<_>, _>>()?.into(),
                $targets.default(),
            )};

            (CallIndirect
                $type_index:ident type_index,
                $table_index:ident table_index,
                $table_byte:ident table_byte
            ) => {
                Instruction::CallIndirect {
                    ty: $type_index,
                    table: $table_index,
                }
            };

            (ReturnCallIndirect
                $type_index:ident type_index,
                $table_index:ident table_index
            ) => {
                Instruction::ReturnCallIndirect {
                    ty: $type_index,
                    table: $table_index,
                }
            };

            (TypedSelect $ty:ident ty) => { Instruction::TypedSelect(Self::convert_type($ty)) };

            (MemoryInit $data_index:ident data_index, $mem:ident mem) => {
                Instruction::MemoryInit {
                    mem: $mem,
                    data_index: $data_index,
                }
            };

            (MemoryCopy $dst_mem:ident dst_mem, $src_mem:ident src_mem) => {
                Instruction::MemoryCopy {
                    src_mem: $src_mem,
                    dst_mem: $dst_mem,
                }
            };

            (TableInit $elem_index:ident elem_index, $table:ident table) => {
                Instruction::TableInit {
                    elem_index: $elem_index,
                    table: $table,
                }
            };

            (TableCopy $dst_table:ident dst_table, $src_table:ident src_table) => {
                Instruction::TableCopy {
                    src_table: $src_table,
                    dst_table: $dst_table,
                }
            };
        }

        wasmparser::for_each_operator!(convert)
    }
}

#[cfg(test)]
mod tests {
    use super::sanitize;
    use crate::Bytecode;

    /// Test if an already sanitized bytecode isn't changed.
    #[test]
    fn doesnt_change_already_sanitized_bytecode() {
        let wat = r#"
            (module
              (type (;0;) (func (param i32) (result i32)))
              (func $my_function (;0;) (type 0) (param i32) (result i32)
                (local i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32)
                global.get $__stack_pointer
                local.set 1
                i32.const 16
                local.set 2
                local.get 1
                local.get 2
                i32.sub
                local.set 3
                local.get 3
                local.get 0
                i32.store
                local.get 3
                local.get 0
                i32.store offset=4
                i32.const 1
                local.set 4
                local.get 3
                local.get 4
                i32.store offset=8
                i32.const 1
                local.set 5
                local.get 0
                local.get 5
                i32.add
                local.set 6
                local.get 6
                local.get 0
                i32.lt_s
                local.set 7
                i32.const 31
                local.set 8
                local.get 6
                local.get 8
                i32.shr_s
                local.set 9
                i32.const -2147483648
                local.set 10
                local.get 9
                local.get 10
                i32.xor
                local.set 11
                local.get 11
                local.get 6
                local.get 7
                select
                local.set 12
                local.get 3
                local.get 12
                i32.store offset=12
                local.get 3
                i32.load offset=12
                local.set 13
                local.get 13
                return
              )
              (table (;0;) 1 1 funcref)
              (memory (;0;) 16)
              (global $__stack_pointer (;0;) (mut i32) i32.const 1048576)
              (global (;1;) i32 i32.const 1048576)
              (global (;2;) i32 i32.const 1048576)
              (export "memory" (memory 0))
              (export "my_function" (func $my_function))
              (export "__data_end" (global 1))
              (export "__heap_base" (global 2))
            )
        "#;

        let input = Bytecode::new(wasmer::wat2wasm(wat.as_bytes()).unwrap().into());
        let output = sanitize(input.clone()).unwrap();

        assert_eq!(input, output);
    }
}
