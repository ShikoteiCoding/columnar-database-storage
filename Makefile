.PHONY: setup sync test demo q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 question-01 question-02 question-03 question-04 question-05 question-06 question-07 question-08 question-09 question-10

UV_RUN = uv run python

setup sync:
	uv sync

test:
	$(UV_RUN) -m unittest discover -s tests -p 'test_*.py'

demo:
	$(UV_RUN) main.py

q1 question-01:
	$(UV_RUN) -m unittest tests.test_question_01_catalog

q2 question-02:
	$(UV_RUN) -m unittest tests.test_question_02_segment_tree

q3 question-03:
	$(UV_RUN) -m unittest tests.test_question_03_statistics_and_pointers

q4 question-04:
	$(UV_RUN) -m unittest tests.test_question_04_blocks

q5 question-05:
	$(UV_RUN) -m unittest tests.test_question_05_column_segments

q6 question-06:
	$(UV_RUN) -m unittest tests.test_question_06_row_groups

q7 question-07:
	$(UV_RUN) -m unittest tests.test_question_07_data_table

q8 question-08:
	$(UV_RUN) -m unittest tests.test_question_08_checkpoint

q9 question-09:
	$(UV_RUN) -m unittest tests.test_question_09_database_facade

q10 question-10:
	$(UV_RUN) -m unittest tests.test_question_10_main_demo