import os
import glob
from itertools import pairwise
from langchain_community.document_loaders import TextLoader
from langchain_community.vectorstores import FAISS
from langchain_core.documents.base import Document
from langchain_huggingface import HuggingFaceEmbeddings
from smolagents import tool, Tool


__all__ = (
    "LogFileFinder",
    "LogRetriever",
    "load_log_summary",
)


class LogFileFinder(Tool):
    name = "find_log_files"
    description = "Finds log files for queried jobs in a run."
    inputs = {
        "query": {
            "type": "string",
            "description": "Query string for finding jobs.",
        },
        "limit": {
            "type": "integer",
            "description": "Limit on number of log files.",
            "nullable": True,
        },
    }
    output_type = "array"

    def __init__(self, submit_dir, df0=None, **kwargs):
        super().__init__(**kwargs)
        self.submit_dir = submit_dir
        if df0 is not None:
            self.df0 = df0
        else:
            from bps_htcondor_agents import extract_jobs_status
            self.df0, _ = extract_jobs_status(submit_dir)

    def forward(self, query: str, limit: int = 10) -> list[str]:
        """Find the job log files given the provided query constraint.  Limit
        to the first 10 files, by default.

        Args:
            query: A query string for selecting the log files for the
                desired jobs, e.g., "ExitCode==1" to find the failed jobs.
            limit: The number of log files to return.
        """
        df = self.df0.query(query).head(limit)

        log_files = []
        for _, row in df.iterrows():
            job_label = row.bps_job_label
            job_name = row.node
            folders = job_name.split(job_label)[-1].lstrip('_').split('_')
            folders.insert(0, job_label)
            log_dir = os.path.join(self.submit_dir, 'jobs', *folders)
            assert os.path.isdir(log_dir)
            log_file = glob.glob(os.path.join(
                log_dir, f'*{int(float(row.job_id))}*.out'))
            assert len(log_file) == 1
            log_files.append(log_file[0])
        return log_files


class LogRetriever(Tool):
    name = "log_retriever"
    description = "Searches through log files to find error patterns or events."
    inputs = {
        "query": {
            "type": "string",
            "description": "Error string to search for.",
        }
    }
    output_type = "string"

    def __init__(self, file_list, **kwargs):
        super().__init__(**kwargs)
        # Load the logs
        documents = []
        for file_path in file_list:
            documents.extend(TextLoader(file_path).load())

        # Split into stanzas delimited by pipetask logging tags.
        self.docs = []
        for doc in documents:
            indexes = []
            lines = doc.page_content.split("\n")
            for i, line in enumerate(lines):
                if line[:4] in ("WARN", "INFO", "ERRO", "VERB"):
                    indexes.append(i)
            indexes.append(len(lines))
            for i, j in pairwise(indexes):
                self.docs.append(
                    Document(
                        metadata=doc.metadata,
                        page_content="\n".join(lines[i:j])
                    )
                )

        # Create the local vector store in RAM
        embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
        self.vector_store = FAISS.from_documents(self.docs, embeddings)

    def forward(self, query: str) -> str:
        # Search the top 3 most relevant log snippets
        results = self.vector_store.similarity_search(query, k=3)
        return "\n---\n".join([res.page_content for res in results])


@tool
def load_log_summary(file_list: list[str]) -> str:
    """Read logs and extract only the relevant Error/Traceback lines
    to save context space.

    Args:
        file_list: List of file paths to read.
    """
    summary = []
    for file_path in file_list:
        # Find lines that start with a logging tag.
        indexes = []
        with open(file_path) as fobj:
            lines = fobj.readlines()
            for i, line in enumerate(lines):
                if line[:4] in ("WARN", "INFO", "ERRO", "VERB"):
                    indexes.append(i)
        indexes.append(len(lines) - 1)
        output_lines = []
        for i, j in pairwise(indexes):
            if lines[i].startswith("ERROR"):
                output_lines.extend(lines[i:j])
                # If no errors found, just take the last few lines for context
                if not output_lines:
                    output_lines = lines[-10:]

                header = f"--- ANALYSIS OF {file_path} ---"
                summary.append(f"{header}\n{output_lines}\n")

    return "\n".join(summary)
