import pytest
from diagnostic_tool.diagnose import main1

def test_cmd():
    main1(['status', '--cluster=127.0.0.1:8181/hw', '--logger_levels=:WARN', '--diff=true', '--conf_file=hosts'])
    # #, '--sdk_log'])
    # print(parser.parse_args(['status', '--diff=true']))

    # log setting by absl logging

    main1(['inspect', 'online'])
    main1(['inspect'])
    with pytest.raises(SystemExit):
        main1(['status', '-h'])
    with pytest.raises(SystemExit):
        main1(['inspect', '-h'])
