"""Device-ID -> appliance mapping for GREEND, and per-appliance row counts.

Names follow the per-house appliance lists in the GREEND paper (Monacchesi et al., 2014), whose order
matches each building's most frequent CSV header (verified with greend_verify_columns.py).
`name` is the paper's label in snake_case (unique within a building, None = not documented in the paper),
`appliance` groups the same kind of equipment across buildings.

Usage: python greend_appliances.py greend_device_profiles.json
"""
import json
import sys
from collections import defaultdict

# building -> [(device_id, name, appliance type)], in column order.
DEVICES = {
    "building0": [
        ("000D6F0002906FA7", "coffee_machine", "coffee_machine"),
        ("000D6F0002907BA2", "washing_machine", "washing_machine"),
        ("000D6F0002907BC8", "radio", "radio"),
        ("000D6F0002907BDF", "water_kettle", "water_kettle"),
        ("000D6F0002907BF5", "fridge_with_freezer", "fridge"),
        ("000D6F0002907C89", "dishwasher", "dishwasher"),
        ("000D6F0002908150", "kitchen_lamp", "lamp"),
        ("000D6F0002908162", "tv", "tv"),
        ("000D6F00029C506A", "vacuum_cleaner", "vacuum_cleaner"),
    ],
    "building1": [
        ("000D6F00036BB04C", "fridge", "fridge"),
        ("000D6F00029C2BD7", "dishwasher", "dishwasher"),
        ("000D6F000353AC8C", "microwave", "microwave"),
        ("000D6F0003562E10", "water_kettle", "water_kettle"),
        ("000D6F0003562C48", "washing_machine", "washing_machine"),
        ("000D6F00029C2984", "radio_with_amplifier", "radio"),
        # Paper says "dryier"; the load (1.5 kW, short, peak 6am) looks like a hairdryer.
        ("000D6F000353AE51", "dryer", "hairdryer"),
        ("000D6F0003562C0F", "kitchenware", "kitchenware"),
        ("000D6F0003562BF6", "bedside_light", "lamp"),
    ],
    "building2": [
        ("000D6F00028EDD69", "tv", "tv"),
        ("000D6F00035616E3", "nas", "nas"),
        ("000D6F00035616EC", "washing_machine", "washing_machine"),
        ("000D6F0003561CCB", "dryer", "tumble_dryer"),
        ("000D6F0003561F97", "dishwasher", "dishwasher"),
        ("000D6F0003562C69", "notebook", "computer"),
        # Kitchenware and bread machine may be swapped (overnight 6am load vs. <=172 W peak).
        ("000D6F0003562C6D", "kitchenware", "kitchenware"),
        ("000D6F0003563110", "coffee_machine", "coffee_machine"),
        ("000D6F0003563119", "bread_machine", "bread_machine"),
    ],
    "building3": [
        ("000D6F00029C1AE6", "entrance_outlet", "outlet"),
        ("000D6F0003561747", "dishwasher", "dishwasher"),
        ("000D6F0003562C4A", "water_kettle", "water_kettle"),
        ("000D6F000356174D", "fridge_without_freezer", "fridge"),
        ("000D6F0003561FFD", "washing_machine", "washing_machine"),
        ("000D6F00035606BF", "hairdryer", "hairdryer"),
        ("000D6F0003561744", "computer", "computer"),
        ("000D6F0003561C12", "coffee_machine", "coffee_machine"),
        ("000D6F00035606C0", "tv", "tv"),
    ],
    "building4": [
        ("000D6F00029C2AEB", "total_outlets", "total_outlets"),
        ("000D6F0003561F98", "total_lights", "total_lights"),
        ("000D6F0003562DF4", "kitchen_tv", "tv"),
        ("000D6F000353AE48", "living_room_tv", "tv"),
        ("000D6F00029C2934", "fridge_with_freezer", "fridge"),
        ("000D6F0003562CA8", "electric_oven", "electric_oven"),
        ("000D6F0003562D88", "computer_with_scanner_and_printer", "computer"),
        ("000D6F0003562C9D", "washing_machine", "washing_machine"),
        ("000D6F00029C1C2B", "hood", "hood"),
        # Not in the paper, 100% null.
        ("000D6F0002792D8A", None, None),
        ("000D6F0002793AA7", None, None),
    ],
    "building5": [
        ("000D6F00029C2918", "plasma_tv", "tv"),
        ("000D6F00029C286E", "lamp", "lamp"),
        ("000D6F00029C44E4", "toaster", "toaster"),
        ("000D6F00029C46A3", "stove", "stove"),
        ("000D6F00029C246A", "iron", "iron"),
        ("000D6F00029C24E1", "computer_with_scanner_and_printer", "computer"),
        ("000D6F00029C46A6", "lcd_tv", "tv"),
        ("000D6F00029C271E", "washing_machine", "washing_machine"),
        ("000D6F00029C46A4", "fridge_with_freezer", "fridge"),
        # Not in the paper, 100% null.
        ("000D6F00023A5F99", None, None),
    ],
    # The paper lists 3 floor totals but the CSVs have 9 devices, only 3 with substantial data.
    # Index 4 (up to 4 kW) is most likely ground + first floor; indices 2 and 6 cannot be told apart,
    # so they get neutral _a/_b suffixes. The other 6 are 86-100% null and not documented.
    "building6": [
        ("000D6F0003562C6E", None, None),
        ("000D6F0003562C89", None, None),
        ("000D6F0003561FAD", "total_garden_and_shelter_or_third_floor_a", "total_floor"),
        ("000D6F0003562D7D", None, None),
        ("000D6F000356311B", "total_ground_and_first_floor", "total_floor"),
        ("000D6F000353ACAF", None, None),
        ("000D6F0002907069", "total_garden_and_shelter_or_third_floor_b", "total_floor"),
        ("000D6F00029C2979", None, None),
        ("000D6F000353AC93", None, None),
    ],
    "building7": [
        ("000D6F00029C2542", "tv_with_decoder", "tv"),
        ("000D6F00036BC42A", "electric_oven", "electric_oven"),
        ("000D6F0003BD8082", "dishwasher", "dishwasher"),
        ("000D6F0003BD8C8E", "hood", "hood"),
        ("000D6F0003BD76C4", "fridge_with_freezer", "fridge"),
        ("000D6F0003BD8103", "kitchen_tv", "tv"),
        ("000D6F0003BD8293", "adsl_modem", "modem"),
        ("000D6F0003BD6E92", "freezer", "freezer"),
        ("000D6F0003B9C636", "laptop_with_scanner_and_printer", "computer"),
    ],
}


def main(profiles_path):
    with open(profiles_path) as f:
        profiles = json.load(f)

    totals = defaultdict(lambda: {"rows": 0, "non_null": 0, "devices": 0, "buildings": set()})
    for building, devices in DEVICES.items():
        by_id = {d["id"]: d for d in profiles[building]["devices"]}
        assert list(by_id) == [d[0] for d in devices], f"column order mismatch in {building}"
        for dev_id, _, appliance in devices:
            if appliance is None:
                continue
            p = by_id[dev_id]
            t = totals[appliance]
            t["rows"] += p["rows"]
            t["non_null"] += round(p["rows"] * (100 - p["null_pct"]) / 100)
            t["devices"] += 1
            t["buildings"].add(building.removeprefix("building"))

    print(f"{'device':<16} {'non-null rows':>14} {'rows incl. null':>16} {'devices':>8}  buildings")
    for name, t in sorted(totals.items(), key=lambda kv: kv[1]["non_null"], reverse=True):
        print(f"{name:<16} {t['non_null']:>14,} {t['rows']:>16,} {t['devices']:>8}  {','.join(sorted(t['buildings']))}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} greend_device_profiles.json")
        sys.exit(-1)
    main(sys.argv[1])
