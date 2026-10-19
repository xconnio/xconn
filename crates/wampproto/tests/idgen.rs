use std::sync::Arc;
use std::thread;

use wampproto::idgen::{ID_MAX, SessionScopeIDGenerator, generate_session_id};

#[test]
fn sequential_counter_restores_wraps_and_rejects_invalid_state() {
    let generator = SessionScopeIDGenerator::new();
    assert_eq!(generator.current_id(), 0);
    assert_eq!(generator.next_id(), 1);
    generator.set_current_id(ID_MAX - 1).unwrap();
    assert_eq!(generator.next_id(), ID_MAX);
    assert_eq!(generator.next_id(), 1);
    assert!(generator.set_current_id(-1).is_err());
    assert!(generator.set_current_id(ID_MAX + 1).is_err());
    assert_eq!(generator.current_id(), 1);
    generator.set_current_id(0).unwrap();
    assert_eq!(generator.next_id(), 1);
}

#[test]
fn concurrent_callers_receive_unique_sequential_ids() {
    let generator = Arc::new(SessionScopeIDGenerator::new());
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let generator = Arc::clone(&generator);
            thread::spawn(move || (0..1000).map(|_| generator.next_id()).collect::<Vec<_>>())
        })
        .collect();
    let mut ids: Vec<_> = workers.into_iter().flat_map(|worker| worker.join().unwrap()).collect();
    ids.sort_unstable();
    assert_eq!(ids, (1..=8000).collect::<Vec<_>>());
    assert_eq!(generator.current_id(), 8000);
}

#[test]
fn generated_session_ids_are_within_the_wamp_domain() {
    for _ in 0..100 {
        assert!((1..=ID_MAX).contains(&generate_session_id().unwrap()));
    }
}
