"""
RID Conversion Tool

Tool to convert between RIDs and coordinates identifying the location of a record.
"""

import config


def coords_to_rid(is_tail: bool, page_num: int, offset: int) -> int:
    """
    Converts the page number and offset to a RID
    """
    tail_bit = 0b1 if is_tail else 0b0
    tail_component = tail_bit << (config.PAGE_NUMBER_BITS + config.OFFSET_BITS)

    page_mask = (page_num << config.OFFSET_BITS) - 1
    page_component = (page_mask & page_num) << config.OFFSET_BITS

    offset_mask = (offset << config.OFFSET_BITS) - 1
    offset_component = offset_mask & offset

    return tail_component | page_component | offset_component


def rid_to_coords(rid: int) -> tuple[bool, int, int]:
    """
    Converts the RID to page number and offset
    """
    tail_bit = (rid >> (config.PAGE_NUMBER_BITS + config.OFFSET_BITS)) & 0b1
    page_num = (rid >> config.OFFSET_BITS) & ((1 << config.PAGE_NUMBER_BITS) - 1)
    offset = rid & ((1 << config.OFFSET_BITS) - 1)

    return bool(tail_bit), page_num, offset
