from smolagents import tool, Tool, CodeAgent
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
    description="Finds log files from a given submit_dir using a log file finder tool.",
)

analysis_agent = CodeAgent(
    model=model,
    tools=[],
    additional_authorized_imports=["os", "glob", "pandas"],
    name="log_analyst",
    description="Analyzes log files provided by a log retriever tool.",
)

manager_agent = CodeAgent(
    model=model,
    additional_authorized_imports=["os", "glob", "pandas"],
    tools=[
        find_failed_runs,
        create_log_file_finder,
        create_log_retriever,
    ],
    managed_agents=[data_agent, analysis_agent],
)

manager_agent.run("""
1. Find runs with failed jobs submitted by user 'lsstsvc1' within the last
   30 days, with string 'DRP' in the bps_run column.
2. For the run with the most failed jobs, create a log file finder tool by
   passing the submit_dir for that run.
3. Assign the log file finder tool to the data agent's tools list.
4. Ask the data agent to find all of the log files for that run for failed jobs
   by passing the query string 'ExitCode==1' to the log file finder tool.
5. Create a log retriever tool by passing that list of log files.
6. Assign the log retriever tool to the analysis agent's tools list.
7. Summarize the errors in those logs.
""")
