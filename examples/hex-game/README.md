<!-- cargo-rdme start -->

# Hex

Hex is a game where player `One` tries to connect the left and right sides of the board and player
`Two` tries to connect top to bottom. The board is rhombic and has a configurable side length `s`.
It consists of `s * s` hexagonal cells, indexed like this:

```rust
(0, 0)      (1, 0)      (2, 0)

      (0, 1)      (1, 1)      (2, 1)

            (0, 2)      (1, 2)      (2, 2)
```

The players alternate placing a stone in their color on an empty cell until one of them wins.

This implementation shows how to write a game that is meant to be played on a shared chain:
Users make turns by submitting operations to the chain, not by sending messages, so a player
does not have to wait for any other chain owner to accept any message.

<!-- cargo-rdme end -->
