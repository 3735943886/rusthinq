# Pinned real driver fixtures

Copied unchanged from the `rusthinq-scripts` working tree at
`54292921c6edc72ea6ec901b1137845bff14e6ae` plus the uncommitted `il_common` migration
(IL moved out of the host); re-pin to that commit once it exists. The GPL license is preserved in
[COPYING](COPYING). Runtime drivers are loaded from the configured directory;
these fixtures are test inputs, not a production fallback.

`aabb_common`, `1WPU4CIGCR__2`, `WBEY3GT` and `D140110` additionally include
the fixed-length EB/EC status-offset refactor from the `rusthinq-scripts`
`dev/0.2` working tree. The hashes below identify the exact fixture contents.

Tests exercise initialization for all eleven models, AABB status/command gating,
including real Pd0F_F pause bytes through the application/device transport,
TLV query/retry effects, and automatic attachment through local provisioning.
The pinned `D140110.test.rhai` supplies nine upstream real cycle frames and expected
values for ThinQ1 B64 replay. The tests construct the Mon envelope; no captured
ThinQ1 TLS session is claimed. They do not assert actual appliance or LG account validation.

| Source | SHA-256 |
| --- | --- |
| `1WPU4CIGCR__2.rhai` | `57b6a1f41dbd0e288ad8ac8157c9d4b4913fd6b5f270cf3ea977bb6009d6bf4c` |
| `2RSFL2DBN3K_Z.rhai` | `e063b5ad3658f1959952911f4b676f6d8910a0293236d616876e60dc652d20e3` |
| `AIR_910604_WW.rhai` | `94f00b3b6b8bb88629cacccdd41ce0004bd6788f411d682addb4f3b39a2a56ab` |
| `CST_570004_WW.rhai` | `6511aa2733235a7df35cef5d0ed2ac53a5b2bf4fe79535914f39b1c87323159a` |
| `D140110.rhai` | `7e59e3c9aa6083beddf37bd5085ff3c40248b16ce37e23d4ff7a5300ce4dc590` |
| `DHUM_056905_WW.rhai` | `8a88cc3b88287f1c7702abf4f112ad1d847fa5f58c16bf97c796259edf25a3d4` |
| `F24VDD.rhai` | `06574ebde4e0958e5a7b122b858d498494aef767a29b55c1247cef8e84a1eae4` |
| `Pd0F_F.rhai` | `2618ea50fe954d8afe3b6b258cfcec07a96c9b8ebf8e93f7a2efab70e2d78025` |
| `RH14_N_KR.rhai` | `e490b5d97cf444fe6fab70dbe6b49fbf169599a35b812e283b9085e63b83241b` |
| `S3BF_POD_DN4.rhai` | `ea2a8c792797ddf3b19efbe58ce0e37364cb9474e8ed7f6c02e99a018b0a7f5b` |
| `WBEY3GT.rhai` | `9cc56dbfbf009f0552f36902dd9feb1928718f7c6a727601f414e06aba85bcd7` |
| `aabb_common.rhai` | `f76b6ec663263405066f3a6ae3e45594eefa9100c3c08ddcec1883d2c34a3d57` |
| `il_common.rhai` | `1c29822e815b65f0fc045433ea2ab318a57d14b064cd792f15dc97066078bffb` |
| `monitoring_common.rhai` | `41b266919c852b0aac35134af1aa45ec7f7968f0952906759f84b977e32f4a60` |
| `tlv_common.rhai` | `ab7fc72b517b54cdb9c69ec8327c7ed5106b73bcd25e26abc45a3f495973a12e` |
| `D140110.test.rhai` | `774277f82a2c7bbd91948140c94e763dc9eb4b7b954f8c340ac28bcf52b50864` |
| `Pd0F_F.test.rhai` | `d221af819e3b97f27ac7c5e44d1ad3fdfb3d9ff917d5f6f8d7ba95c138ca3932` |
