import pytest
from diagnostic_tool.diagnose import parse_arg, main
from absl import flags
from .case_conf import OpenMLDB_ZK_CLUSTER

def test_argparse():
    cluster_arg = f'--cluster={OpenMLDB_ZK_CLUSTER}'
    # -1 (warning),0 (info), 1 (debug), app default is -1
    args = parse_arg(['status', cluster_arg, '--diff', '--conf_file=hosts', '-v=0'])
    assert flags.FLAGS.cluster == OpenMLDB_ZK_CLUSTER
    assert args.diff == True
    flags.FLAGS['verbosity'].unparse()

    args = parse_arg(['--sdk_log'])
    assert not hasattr(args, 'command'), 'no subcommand'
    assert flags.FLAGS.sdk_log
    flags.FLAGS['sdk_log'].unparse() # reset it, don't effect tests bellow
    # log setting by module, std logging name:level
    # parse_arg(['test', cluster_arg, '-v=-1', '--logger_levels=foo.bar:INFO'])
    # flags.FLAGS['logger_levels'].unparse()
    # flags.FLAGS['verbosity'].unparse()

    new_addr = '111.0.0.1:8181/openmldb'
    cluster_arg = f'--cluster={new_addr}'
    parse_arg(['inspect', 'online', cluster_arg])
    assert flags.FLAGS.cluster == new_addr

    parse_arg(['remote', '-VCL', '-v=0'])
    flags.FLAGS['verbosity'].unparse()

    # with pytest.raises(SystemExit):
    #     parse_arg(['status', '-h'])
    # with pytest.raises(SystemExit):
    #     parse_arg(['inspect', '-h'])
    # with pytest.raises(SystemExit):
    #     parse_arg(['inspect', 'online', '-h'])
    # with pytest.raises(SystemExit):
    #     parse_arg(['test', '-h'])
    # with pytest.raises(SystemExit):
    #     parse_arg(['remote', '-h'])
    # with pytest.raises(SystemExit):
    #     parse_arg(['--helpfull'])

def test_cmd():
    # singleton connection
    cluster_arg = f'--cluster={OpenMLDB_ZK_CLUSTER}'
    main(['status', cluster_arg,])
    main(['status', '--cluster=foo/bar',]) # still connect to OpenMLDB_ZK_CLUSTER
