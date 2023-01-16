import pytest
from diagnostic_tool.diagnose import main1

def test_cmd():
    cluster_arg = '--cluster=127.0.0.1:8181/hw'
    main1(['status', cluster_arg, '--logger_levels=:WARN', '--diff=true', '--conf_file=hosts'])
    # #, '--sdk_log'])
    # (['status', '--diff=true']))
    main1(['test', cluster_arg])
    # log setting by absl logging

    main1(['inspect', 'online'])
    main1(['inspect'])
    with pytest.raises(SystemExit):
        main1(['status', '-h'])
    with pytest.raises(SystemExit):
        main1(['inspect', '-h'])
    with pytest.raises(SystemExit):
        main1(['test', '-h'])
