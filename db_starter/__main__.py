import sys
import signal
import logging
import argparse

import kubernetes

from . import listener

from .starter import Starter

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--log-level', default='INFO')
    parser.add_argument('--in-cluster', action='store_true', help='configure with in cluster kubeconfig')
    parser.add_argument('--listen-address', default='0.0.0.0')
    parser.add_argument('--listen-port', type=int, default=5432)
    parser.add_argument('--src-pvc-namespace', default='default')
    parser.add_argument('--src-pvc-name', default='postgres')
    parser.add_argument('--target-namespace', default='default')
    parser.add_argument('--target-pvc-name', default='postgres-clone')
    parser.add_argument('--target-deployment-name', default='postgres-clone')
    parser.add_argument('--self-deployment-name', default='db-starter')
    parser.add_argument('--service-name', default='postgres-clone')
    parser.add_argument('--gcp-project', required=True)
    parser.add_argument('--gcp-zone', required=True)
    args = parser.parse_args()

    logging.basicConfig(format='%(message)s', level=args.log_level)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    if args.in_cluster:
        kubernetes.config.load_incluster_config()
    else:
        configuration = kubernetes.client.Configuration()
        configuration.host = 'http://127.0.0.1:8001'
        kubernetes.client.Configuration.set_default(configuration)

    starter = Starter()
    starter.src_pvc_namespace = args.src_pvc_namespace
    starter.src_pvc_name = args.src_pvc_name
    starter.target_namespace = args.target_namespace
    starter.target_pvc_name = args.target_pvc_name
    starter.target_deployment_name = args.target_deployment_name
    starter.self_deployment_name = args.self_deployment_name
    starter.service_name = args.service_name
    starter.gcp_project = args.gcp_project
    starter.gcp_zone = args.gcp_zone

    starter.disengage()
    listener.listen(args.listen_address, args.listen_port, starter)


def shutdown(signum, frame):
    log.info('Shutting down')
    sys.exit(0)


if __name__ == '__main__':
    main()
