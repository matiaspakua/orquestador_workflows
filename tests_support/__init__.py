"""Shared support library for the producer-consumer integration suite (feature 005).

This package is imported by both ``producer/tests`` and ``consumer/tests`` so the
Kafka topic lifecycle manager, message-contract helpers, client harnesses, and
mock orchestrator are defined once. The test runner puts the repo root on
``PYTHONPATH`` (see docker-compose.test.yml / pytest.ini) so ``import
tests_support`` resolves from either test tree.
"""
