import os
from smolagents import CodeAgent
from bps_htcondor_agents import LogFileFinder, LogRetriever


class RagLogManager:
    def __init__(self, model, submit_dir, run_summary):
        self.model = model
        log_file_finder = LogFileFinder(submit_dir, df0=run_summary)

        self.data_agent = CodeAgent(
            model=model,
            tools=[log_file_finder],
            name="log_file_finder",
        )

    def run_analysis(self, job_query, analysis_instructions):
        file_list = self.data_agent.run(
            f"Find log files using the job query '{job_query}'. "
            "Return a list of file paths."
        )
        for file_path in file_list:
            assert os.path.isfile(file_path)

        rag_tool = LogRetriever(file_list)

        analysis_agent = CodeAgent(
            model=self.model,
            tools=[rag_tool],
            name="log_analyst",
        )

        return analysis_agent.run(analysis_instructions)
