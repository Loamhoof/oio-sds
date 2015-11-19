#!/usr/bin/env python

from oio.blob.rebuilder import BlobRebuilder
from oio.common.daemon import run_daemon
from oio.common.utils import parse_options
from optparse import OptionParser


if __name__ == '__main__':
    parser = OptionParser("%prog CONFIG [options]")
    config, options = parse_options(parser)
    run_daemon(BlobRebuilder, config, **options)
