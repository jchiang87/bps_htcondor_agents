from smolagents import CodeAgent
from models import get_model
from bps_htcondor_agents import (
    create_log_file_finder,
    create_log_retriever,
    find_failed_runs
)


model_id = "claude-4-5-sonnet"
model = get_model(model_id, test_connection=True)

data_agent = CodeAgent(
    model=model,
    tools=[],
    additional_authorized_imports=["os", "glob", "pandas"],
    name="log_file_finder",
    description=("Finds log files from a given submit_dir using a "
                 "log file finder tool."),
    instructions=("Use the log file finder tool to find all of the log files "
                  "for failed runs by passing the query string 'ExitCode==1' "
                  "to the tool."),
)

analysis_agent = CodeAgent(
    model=model,
    tools=[],
    additional_authorized_imports=["os", "glob", "pandas"],
    name="log_analyst",
    description="Analyzes log files provided by a log retriever tool.",
    instructions=("Use the log retriever tool to process the information "
                  "in the logs in the provided file list."),
)

manager = CodeAgent(
    model=model,
    additional_authorized_imports=["os", "glob", "pandas"],
    tools=[
        find_failed_runs,
        create_log_file_finder,
        create_log_retriever,
    ],
    managed_agents=[data_agent, analysis_agent],
    instructions="""Perform the following workflow:

    1. Find the run with failed jobs for the provided criteria using
       the find failed runs tool.
    2. Create a log file finder tool by passing the submit_dir for the
       designated run.
    3. Assign the log file finder tool to the data agent's tool list.
    4. Ask the data agent to find all of the log files for the failed
       runs.
    5. Create a log retriever tool by passing that list of log files.
    6. Assign the log retriever tool to the analysis agent's tool list.
    7. Summarize the errors in the retrieved logs."""
)

manager.run("""
Find runs with failed jobs submitted by user 'lsstsvc1' within the
last 30 days, with string 'DRP' in the bps_run column.  For the run
with the most failed jobs, summarize the errors in those logs.
""")
