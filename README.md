# deepcopy

A directory copier that handles cloud-backed files properly.

Designed specifically around limitations with moving files kept online by OneDrive.
Built for Windows, but functional in Linux

## Install

From [crates.io](https://crates.io/crates/deepcopy):

```
cargo install deepcopy
```

Or download a prebuilt binary from the [releases page](https://github.com/lo9ud/deepcopy/releases).

## Usage

```
deepcopy <SOURCE> <DEST>
```

By default you are asked what to do about each file that already exists at the destination. Use
`--conflict skip` or `--conflict overwrite` to set it explicitly.

---

Licensed under the [MIT License](LICENSE). Contributions welcome.
