# aurelia-steam-cdn

Steam content-delivery and depot download library.

This is the [Aurelia](https://github.com/Drackrath/Aurelia) fork of
[steam-cdn](https://github.com/OccamyLab/steam-cdn). It differs from upstream in
these ways:

- Built on [aurelia-steam-vent](https://codeberg.org/Drackrath/aurelia-steam-vent)
  0.6 (a steam-vent fork with per-session logon IDs) and reqwest 0.13, so it
  takes an `aurelia-steam-vent` connection. Protos come from `steam-vent-proto`.
- Supports Steam's VSZ/Zstd chunk containers.
- Downloads chunks in parallel, with host failover and retries.
- Skips existing chunks only when their SHA matches, not on file length alone.
- Creates zero-byte files, directories and symlinks from depot entries, and
  applies executable flags.
- Reports typed disk-full, offline and cancelled errors, plus progress updates.

## License

This fork is licensed under the GNU General Public License v3.0 or later, see
`LICENSE`.

It is based on [steam-cdn](https://github.com/OccamyLab/steam-cdn), which is
licensed under the Apache License 2.0. Upstream's license text is kept in
`LICENSE-APACHE`.
