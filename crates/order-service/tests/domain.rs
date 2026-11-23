// SPDX-License-Identifier: MIT

//! The domain rules: the lifecycle, checkout's validation and pricing, the
//! guest email, cancellations' reasons and actors, the operator gate.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use order_service::domain::{
    Actor, Address, ItemsError, Line, NewItem, OperatorAuth, OrderEvent, OrderStatus,
    ReconciliationStatus, Rejected, Unauthorized, guest_email, parse_uuid, total_cents,
    validate_items,
};

const ALL: [OrderStatus; 6] = [
    OrderStatus::Confirmed,
    OrderStatus::PaymentFailed,
    OrderStatus::Cancelled,
    OrderStatus::Authorized,
    OrderStatus::Dispatched,
    OrderStatus::DispatchFailed,
];

#[test]
fn statuses_round_trip_and_nothing_else_parses() {
    for status in ALL {
        assert_eq!(OrderStatus::parse(status.as_str()), Some(status));
    }
    assert_eq!(OrderStatus::parse("pending"), None);
    for status in [ReconciliationStatus::Open, ReconciliationStatus::Resolved] {
        assert_eq!(ReconciliationStatus::parse(status.as_str()), Some(status));
    }
    assert_eq!(ReconciliationStatus::parse("closed"), None);
}

#[test]
fn captures_move_authorized_or_dispatched_orders_everything_else_authorized_ones() {
    let reason = "capture_declined".to_owned();
    let cases = [
        (OrderEvent::PaymentCaptured, OrderStatus::Confirmed, 2),
        (
            OrderEvent::PaymentFailed(reason),
            OrderStatus::PaymentFailed,
            2,
        ),
        (OrderEvent::AuthorizationExpired, OrderStatus::Cancelled, 1),
        (OrderEvent::DispatchConfirmed, OrderStatus::Dispatched, 1),
        (OrderEvent::DispatchFailed, OrderStatus::DispatchFailed, 1),
        (OrderEvent::CustomerCancelled, OrderStatus::Cancelled, 1),
        (
            OrderEvent::OperatorCancelled("op".to_owned()),
            OrderStatus::Cancelled,
            1,
        ),
    ];
    for (event, target, allowed) in cases {
        assert_eq!(event.target(), target);
        assert_eq!(event.allowed_from().len(), allowed, "{}", event.label());
        assert_eq!(event.transition(OrderStatus::Authorized), Ok(target));
        assert_eq!(
            event.transition(OrderStatus::Cancelled),
            Err(Rejected::Illegal {
                from: OrderStatus::Cancelled,
                to: target
            })
        );
    }
    assert_eq!(
        OrderEvent::PaymentCaptured.transition(OrderStatus::Dispatched),
        Ok(OrderStatus::Confirmed)
    );
}

#[test]
fn labels_name_the_events() {
    let labels: Vec<_> = [
        OrderEvent::PaymentCaptured,
        OrderEvent::PaymentFailed(String::new()),
        OrderEvent::AuthorizationExpired,
        OrderEvent::DispatchConfirmed,
        OrderEvent::DispatchFailed,
        OrderEvent::CustomerCancelled,
        OrderEvent::OperatorCancelled(String::new()),
    ]
    .iter()
    .map(OrderEvent::label)
    .collect();
    assert_eq!(
        labels,
        [
            "PaymentCaptured",
            "PaymentFailed",
            "AuthorizationExpired",
            "DispatchConfirmed",
            "DispatchFailed",
            "CustomerCancelled",
            "OperatorCancelled"
        ]
    );
}

#[test]
fn cancellations_have_a_reason_and_an_actor_other_events_none() {
    let actor = |kind: &str, id: &str| Actor {
        kind: kind.to_owned(),
        id: id.to_owned(),
    };
    assert_eq!(
        OrderEvent::CustomerCancelled.cancellation_reason(),
        Some("customer")
    );
    assert_eq!(
        OrderEvent::CustomerCancelled.actor("c1"),
        Some(actor("customer", "c1"))
    );
    let operator = OrderEvent::OperatorCancelled("dev-operator".to_owned());
    assert_eq!(operator.cancellation_reason(), Some("operator"));
    assert_eq!(
        operator.actor("c1"),
        Some(actor("operator", "dev-operator"))
    );
    assert_eq!(
        OrderEvent::AuthorizationExpired.cancellation_reason(),
        Some("auth_expired")
    );
    assert_eq!(
        OrderEvent::AuthorizationExpired.actor("c1"),
        Some(actor("system", "payment-service"))
    );
    assert_eq!(OrderEvent::DispatchFailed.cancellation_reason(), None);
    assert_eq!(OrderEvent::PaymentCaptured.actor("c1"), None);
}

fn address() -> Address {
    Address {
        line1: "1 Example Street".to_owned(),
        line2: None,
        city: "London".to_owned(),
        postal_code: "EC1A 1AA".to_owned(),
        country: "GB".to_owned(),
    }
}

#[test]
fn an_address_needs_line1_city_postal_code_and_an_alpha2_country() {
    assert_eq!(address().validate(), Ok(()));
    let broken = [
        (
            Address {
                line1: " ".to_owned(),
                ..address()
            },
            "line1 is required",
        ),
        (
            Address {
                city: String::new(),
                ..address()
            },
            "city is required",
        ),
        (
            Address {
                postal_code: " ".to_owned(),
                ..address()
            },
            "postalCode is required",
        ),
        (
            Address {
                country: "gb".to_owned(),
                ..address()
            },
            "country must be an ISO 3166-1 alpha-2 code",
        ),
        (
            Address {
                country: "GBR".to_owned(),
                ..address()
            },
            "country must be an ISO 3166-1 alpha-2 code",
        ),
    ];
    for (address, reason) in broken {
        assert_eq!(address.validate(), Err(reason.to_owned()));
    }
}

#[test]
fn a_guest_email_is_trimmed_and_lower_cased_a_blank_one_absent() {
    assert_eq!(guest_email(None), Ok(None));
    assert_eq!(guest_email(Some("  ")), Ok(None));
    assert_eq!(
        guest_email(Some(" Guest@Example.COM ")),
        Ok(Some("guest@example.com".to_owned()))
    );
    assert_eq!(guest_email(Some("a@b.c.d")), Ok(Some("a@b.c.d".to_owned())));
    for bad in [
        "not-an-email",
        "a@b",
        "a@.b",
        "a@b.",
        "@b.c",
        "a b@c.d",
        "a@b@c.d",
    ] {
        assert_eq!(
            guest_email(Some(bad)),
            Err("email must be a valid email address".to_owned()),
            "{bad}"
        );
    }
    let long = format!("{}@example.com", "a".repeat(250));
    assert!(guest_email(Some(&long)).is_err());
}

fn item(quantity: i32, unit_price_cents: i32) -> NewItem {
    NewItem {
        sku: "SKU-A".to_owned(),
        product_name: "A".to_owned(),
        unit_price_cents,
        quantity,
    }
}

#[test]
fn items_need_one_line_each_quantity_positive_each_price_not_negative() {
    assert_eq!(validate_items(&[]), Err(ItemsError::Empty));
    assert_eq!(validate_items(&[item(1, 0)]), Ok(()));
    assert_eq!(
        validate_items(&[item(0, -1)]),
        Err(ItemsError::Invalid(
            "quantity must be positive, got 0 for sku 'SKU-A'".to_owned()
        ))
    );
    assert_eq!(
        validate_items(&[item(1, -1)]),
        Err(ItemsError::Invalid(
            "unitPriceCents must be non-negative, got -1 for sku 'SKU-A'".to_owned()
        ))
    );
}

#[test]
fn the_total_is_summed_wide_and_must_fit_an_int() {
    let line = |unit_price_cents, quantity| Line {
        unit_price_cents,
        quantity,
    };
    assert_eq!(total_cents(&[line(1299, 2), line(500, 1)]), Ok(3098));
    assert_eq!(total_cents(&[]), Ok(0));
    assert_eq!(
        total_cents(&[line(i32::MAX, 2)]),
        Err("order total exceeds the wire contract's Int range: 4294967294".to_owned())
    );
    assert_eq!(
        total_cents(&[line(-1, 1)]),
        Err("unitPriceCents must be non-negative, got -1".to_owned())
    );
    assert_eq!(
        total_cents(&[line(1, -1)]),
        Err("quantity must be non-negative, got -1".to_owned())
    );
}

#[test]
fn uuids_are_java_canonical() {
    assert!(parse_uuid("3f2b8c1e-7a4d-4e9b-9c2a-1d5e6f7a8b90").is_some());
    assert!(parse_uuid("3f2b8c1e7a4d4e9b9c2a1d5e6f7a8b90").is_none());
    assert!(parse_uuid("not-a-uuid").is_none());
}

#[test]
fn no_header_is_the_customer_a_matching_bearer_the_operator() {
    let auth = OperatorAuth::new(" dev-operator-token ", " dev-operator ");
    assert_eq!(auth.authenticate(None), Ok(None));
    assert_eq!(
        auth.authenticate(Some("bearer  dev-operator-token ")),
        Ok(Some("dev-operator"))
    );
    for bad in [
        "Bearer wrong",
        "Basic dev-operator-token",
        "Bearer ",
        "Beareré x",
    ] {
        assert_eq!(auth.authenticate(Some(bad)), Err(Unauthorized), "{bad}");
    }
    assert_eq!(auth.require_operator(None), Err(Unauthorized));
    assert_eq!(
        auth.require_operator(Some("Bearer dev-operator-token")),
        Ok("dev-operator")
    );
    let disabled = OperatorAuth::new(" ", "dev-operator");
    assert_eq!(
        disabled.require_operator(Some("Bearer anything")),
        Err(Unauthorized)
    );
}
