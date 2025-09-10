from apps.dlq_handler.backoff import schedule, plan

def test_schedule_caps():
    assert schedule(1, base=5, mult=3) == 5
    assert schedule(2, base=5, mult=3) == 15
    assert schedule(10, base=5, mult=3, cap=60) == 60

def test_plan_len():
    p = plan(5)
    assert len(p) == 5
