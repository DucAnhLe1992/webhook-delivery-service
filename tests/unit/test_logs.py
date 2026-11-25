import logging
import re

from webhook_service.domain import AttemptOutcome
from webhook_service.logs import JsonFormatter, TextFormatter


def attempt_record(**fields) -> logging.LogRecord:
    record = logging.LogRecord(
        "worker", logging.INFO, __file__, 1, "%s attempt %d: delivered", ("abc", 2), None
    )
    record.__dict__.update(fields)
    return record


def test_a_json_line_has_the_message_and_every_field(json_logs):
    logging.getLogger("worker").info(
        "%s attempt %d: delivered",
        "abc",
        2,
        extra={
            "event": "attempt_finished",
            "delivery_id": "abc",
            "outcome": AttemptOutcome.DELIVERED,
        },
    )

    [line] = json_logs()

    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", line.pop("time"))
    assert line == {
        "level": "info",
        "process": "api",
        "logger": "worker",
        "message": "abc attempt 2: delivered",
        "event": "attempt_finished",
        "delivery_id": "abc",
        "outcome": "delivered",
    }


def test_a_json_line_carries_the_traceback_of_an_error(json_logs):
    try:
        raise RuntimeError("a bug")
    except RuntimeError:
        logging.getLogger("worker").exception("unexpected error")

    [line] = json_logs()

    assert line["level"] == "error"
    assert "Traceback" in line["exception"]
    assert "RuntimeError: a bug" in line["exception"]


def test_a_text_line_shows_only_the_time_the_process_and_the_message():
    line = TextFormatter("worker").format(attempt_record(event="attempt_finished", outcome="x"))

    # dev/demo.py reads the worker's lines in this shape.
    assert re.fullmatch(r"\d\d:\d\d:\d\d \[worker\] abc attempt 2: delivered", line)


def test_fields_that_json_cannot_hold_are_written_as_text():
    line = JsonFormatter("worker").format(attempt_record(when=object.__new__(object)))

    assert '"when": "<object object at' in line
