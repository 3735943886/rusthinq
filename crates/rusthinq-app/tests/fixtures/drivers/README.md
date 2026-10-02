# Pinned real driver fixtures

Copied unchanged from `rusthinq-scripts` revision
`54292921c6edc72ea6ec901b1137845bff14e6ae`. The GPL license is preserved in
[COPYING](COPYING). Runtime drivers are loaded from the configured directory;
these fixtures are test inputs, not a production fallback.

Tests exercise initialization for all eleven models, AABB status/command gating,
including real Pd0F_F pause bytes through the application/device transport,
TLV query/retry effects, and automatic attachment through local provisioning.
The pinned `D140110.test.rhai` supplies nine upstream real cycle frames and expected
values for ThinQ1 B64 replay. The tests construct the Mon envelope; no captured
ThinQ1 TLS session is claimed. They do not assert actual appliance or LG account validation.

| Source | SHA-256 |
| --- | --- |
| `1WPU4CIGCR__2.rhai` | `61555e117e091da549a657e0d95955b89f11cd3230ceefc0de169109433f44e9` |
| `2RSFL2DBN3K_Z.rhai` | `44d66bab2881a1a40a6872dea1ccd9b7465df076e5c8cf39bd89ce06f6127db2` |
| `AIR_910604_WW.rhai` | `dba2593b63c40de088eb2781733469dd3ad4078bbcbf2a8767a032f8b29a9b72` |
| `CST_570004_WW.rhai` | `d56452bb2fdc6777cf28fc9443219c325a5f83a9d0b1b3aeefc8a0120fe6c9e2` |
| `D140110.rhai` | `65c6a26eb6d9b8d408bc51b19ddf598176570f2976c51421e48f626e4442ce89` |
| `DHUM_056905_WW.rhai` | `011fc5c64facaa5abe43fa813a4c5b2c5a5d85a412b8352949d959de9935bc49` |
| `F24VDD.rhai` | `b937ce6a353539bc1788eae0117d024dc857ea6214dffe83d41e3c5f23d3ff41` |
| `Pd0F_F.rhai` | `fca91e7d1556830bf0318916794b69f56c193c9fa6bcb04ba8e23e5dc284aa37` |
| `RH14_N_KR.rhai` | `c7539ad12e4604db9457164142980244cfe36f1e6231b0dfb7150c8916581e25` |
| `S3BF_POD_DN4.rhai` | `b3cdb1b793528fc81fd012e53c4d3d6322b2f0d8231f96559e60a309fdcd02d2` |
| `WBEY3GT.rhai` | `41a0f21cdd871277278ce45204ddcfc07cd6ecd4e360ee75099969a9cfd7c868` |
| `aabb_common.rhai` | `fa6431795f3c6ba58234a59c65dcc02740696336fd89bbd9a9b7ee077abf776c` |
| `monitoring_common.rhai` | `41b266919c852b0aac35134af1aa45ec7f7968f0952906759f84b977e32f4a60` |
| `tlv_common.rhai` | `8717043714545beefdf1f05af0a528f43cafd61623a0f5e2d7ce62ea77ec7433` |
| `D140110.test.rhai` | `f4d200dbab94906eaa0670ab9353e45b43adf6c7ca252378279b4cefa496756b` |
| `Pd0F_F.test.rhai` | `2c507e782d6780d636d556cc493b7f3a2c27c6582cd326c1af4af731bff2f7ec` |
