from pydantic import BaseModel, NonNegativeFloat, NonNegativeInt, root_validator, validator


class Customer(BaseModel):
    customer_id: str
    days_since_first_order: NonNegativeInt
    days_since_last_order: NonNegativeInt
    is_newsletter_subscriber: bool
    orders: NonNegativeInt
    items: NonNegativeInt
    cancels: NonNegativeInt
    returns: NonNegativeInt
    different_addresses: NonNegativeInt
    shipping_addresses: NonNegativeInt
    devices: NonNegativeInt
    vouchers: NonNegativeInt
    cc_payments: NonNegativeInt
    paypal_payments: NonNegativeInt
    afterpay_payments: NonNegativeInt
    apple_payments: NonNegativeInt
    female_items: NonNegativeInt
    male_items: NonNegativeInt
    unisex_items: NonNegativeInt
    wapp_items: NonNegativeInt
    wftw_items: NonNegativeInt
    mapp_items: NonNegativeInt
    wacc_items: NonNegativeInt
    macc_items: NonNegativeInt
    mftw_items: NonNegativeInt
    wspt_items: NonNegativeInt
    mspt_items: NonNegativeInt
    curvy_items: NonNegativeInt
    sacc_items: NonNegativeInt
    msite_orders: NonNegativeInt
    desktop_orders: NonNegativeInt
    android_orders: NonNegativeInt
    ios_orders: NonNegativeInt
    other_device_orders: NonNegativeInt
    work_orders: NonNegativeInt
    home_orders: NonNegativeInt
    parcelpoint_orders: NonNegativeInt
    other_collection_orders: NonNegativeInt
    average_discount_onoffer: NonNegativeFloat
    average_discount_used: NonNegativeFloat
    revenue: NonNegativeFloat

    @validator("cancels", "returns")
    def check_less_than_orders(cls, v, values, field, **kwargs):
        if v > values["orders"]:
            raise ValueError(f"{field.name}_more_than_orders")
        return v

    @root_validator
    def check_days_since_last_order_lt_first_order(cls, values):
        days_since_last_order, days_since_first_order = values.get("days_since_last_order"), values.get(
            "days_since_first_order"
        )
        if days_since_last_order > days_since_first_order:
            values["days_since_last_order"] = -1
            values["days_since_first_order"] = -1
        return values
