from bps_htcondor_agents import extract_jobs_status

#submit_dir = ("/sdf/data/rubin/user/jchiang/htcondor_tests"
#              "/submit/u/jchiang/htcondor_batch_submit_tests_w_2026_08"
#              "/20260220T203232Z/")

submit_dir = ("/sdf/data/rubin/shared/campaigns/LSSTCam/DM-54210"
              "/submit/LSSTCam/runs/DRP/DP2-pilot/v30_0_4_rc1/DM-54210"
              "/stage3/20260220T185213Z/")

run_summary, jobs = extract_jobs_status(submit_dir)
