from models import get_model
from rag_log_server import RagLogManager
from jobs_status import submit_dir, run_summary

model_id = "claude-4-5-sonnet"
model = get_model(model_id, test_connection=True)

manager = RagLogManager(model, submit_dir, run_summary)

job_query = "ExitCode==1"
analysis_instructions = "Summarize the errors in the logs"
#manager.run_analysis(job_query, analysis_instructions)
