"""Side-effect-free public Robinhood Chain LP protocol registry.

Only public chain identifiers and reviewed protocol deployments belong here.
Runtime configuration, credentials, wallet identities, and operator policy must
not be added to this module.
"""
from __future__ import annotations

CHAIN_ID = 4663
NATIVE = "0x0000000000000000000000000000000000000000"
WETH = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
WETH_USDG_POOL = "0x52e65b17fb6e5ba00ed806f37afcd2daa50271ca"

POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
STATE_VIEW = "0xf3334192d15450cdd385c8b70e03f9a6bd9e673b"
SLIPSTREAM_FACTORY = "0x1ac9db4a2608ba45d6127b1737949b51bb54b7f3"
UNISWAP_V3_FACTORY = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
PANCAKE_V3_FACTORY = "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"
EXTENDED_V3_FACTORY = "0xece6ecd61177336ea6fb9b17937ac439d85ee20b"

V2_FACTORIES = frozenset({
    "0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f",
    "0xdaa80fe4ee10de529d98ea4bde15bcaf7f7324b3",
    "0x0d1ebb179cdbca88d74c923c4255cb2b17474afd",
})
V3_FACTORIES = frozenset({
    UNISWAP_V3_FACTORY,
    PANCAKE_V3_FACTORY,
    EXTENDED_V3_FACTORY,
})
CONCENTRATED_FACTORIES = frozenset({SLIPSTREAM_FACTORY, *V3_FACTORIES})
CREATION_EMITTERS = frozenset({POOL_MANAGER, SLIPSTREAM_FACTORY, *V2_FACTORIES, *V3_FACTORIES})

__all__ = [
    "CHAIN_ID",
    "CONCENTRATED_FACTORIES",
    "CREATION_EMITTERS",
    "EXTENDED_V3_FACTORY",
    "NATIVE",
    "PANCAKE_V3_FACTORY",
    "POOL_MANAGER",
    "SLIPSTREAM_FACTORY",
    "STATE_VIEW",
    "UNISWAP_V3_FACTORY",
    "USDG",
    "V2_FACTORIES",
    "V3_FACTORIES",
    "WETH",
    "WETH_USDG_POOL",
]
