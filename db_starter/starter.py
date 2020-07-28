import enum
import time
import logging
import datetime
import threading
import contextlib

import kubernetes
import googleapiclient.discovery

from .activity_watcher import ActivityWatcher

log = logging.getLogger(__name__)


class State(enum.IntEnum):
    DISENGAGED = 0
    ENGAGING = 1
    ENGAGED = 2
    DISENGAGING = 3


class Starter:
    def __init__(self):
        self.v1 = kubernetes.client.CoreV1Api()
        self.apps_v1 = kubernetes.client.AppsV1Api()
        self.compute = googleapiclient.discovery.build('compute', 'v1', cache_discovery=False)

        self.state = State.DISENGAGED
        self.activity_watcher = None

        self.src_pvc_namespace = None
        self.src_pvc_name = None
        self.target_namespace = None
        self.target_pvc_name = None
        self.target_deployment_name = None
        self.self_deployment_name = None
        self.service_name = None
        self.gcp_project = None
        self.gcp_zone = None

    def engage(self):
        if self.state != State.DISENGAGED:
            log.info('Not engaging, already %s', self.state.name)
            return

        self.state = State.ENGAGING
        log.info('Engaging!')

        src_pvc = self.v1.read_namespaced_persistent_volume_claim(self.src_pvc_name, self.src_pvc_namespace)
        src_pv = self.v1.read_persistent_volume(src_pvc.spec.volume_name)
        src_disk_name = src_pv.spec.gce_persistent_disk.pd_name

        log.info('src PV: %s', src_pv.metadata.name)
        log.info('src PD: %s', src_disk_name)

        src_disk = self.compute.disks().get(
            project=self.gcp_project,
            zone=self.gcp_zone,
            disk=src_disk_name,
        ).execute()

        snapshots = self.list_snapshots(src_disk['selfLink'])

        log.info('Got %d snapshots', len(snapshots))

        snapshot = snapshots[0]

        log.info('Using snapshot "%s" from %s', snapshot['name'], snapshot['creationTimestamp'])

        disk = {
            'name': f'db-starter--{src_pv.metadata.name}',
            'sourceSnapshot': snapshot['selfLink'],
            'type': f'projects/{self.gcp_project}/zones/{self.gcp_zone}/diskTypes/pd-ssd',
        }

        log.info('Creating disk %s...', disk['name'])

        self.compute.disks().insert(
            project=self.gcp_project,
            zone=self.gcp_zone,
            body=disk,
        ).execute()
        # result = self.wait_for_operation(operation['name'])

        target_pv = self.v1.create_persistent_volume({
            'apiVersion': 'v1',
            'kind': 'PersistentVolume',
            'metadata': {
                'name': disk['name'],
            },
            'spec': {
                'accessModes': [
                    'ReadWriteOnce',
                ],
                'capacity': src_pv.spec.capacity,
                'claimRef': {
                    'apiVersion': 'v1',
                    'kind': 'PersistentVolumeClaim',
                    'namespace': self.target_namespace,
                    'name': self.target_pvc_name,
                },
                'gcePersistentDisk': {
                    'pdName': disk['name'],
                },
                'persistentVolumeReclaimPolicy': 'Delete',
            },
        })

        log.info('target PV: %s created', target_pv.metadata.name)

        target_pvc = self.v1.create_namespaced_persistent_volume_claim(self.target_namespace, {
            'apiVersion': 'v1',
            'kind': 'PersistentVolumeClaim',
            'metadata': {
                'name': self.target_pvc_name,
            },
            'spec': {
                'accessModes': [
                    'ReadWriteOnce',
                ],
                'resources': {
                    'requests': src_pv.spec.capacity,
                },
            },
        })

        log.info('target PVC: %s/%s created', target_pvc.metadata.namespace, target_pvc.metadata.name)

        deployment = self.apps_v1.read_namespaced_deployment(self.target_deployment_name, self.target_namespace)
        deployment.spec.replicas = 1
        self.apps_v1.patch_namespaced_deployment(self.target_deployment_name, self.target_namespace, deployment)
        log.info('Deployment %s scaled', self.target_deployment_name)

        service = self.v1.read_namespaced_service(self.service_name, self.target_namespace)
        service.spec.selector = deployment.spec.selector.match_labels
        self.v1.patch_namespaced_service(self.service_name, self.target_namespace, service)
        log.info('Service %s updated', self.service_name)

        service_dns_name = f'{self.service_name}.{self.target_namespace}.svc.cluster.local'
        self.activity_watcher = ActivityWatcher(service_dns_name, datetime.timedelta(minutes=10), self.disengage)
        activity_watcher_thread = threading.Thread(target=self.activity_watcher.start, name='activity_watcher')
        activity_watcher_thread.daemon = True
        activity_watcher_thread.start()

        self.state = State.ENGAGED

    def disengage(self):
        self.state = State.DISENGAGING
        log.info('Disengaging!')

        if self.activity_watcher:
            self.activity_watcher.cancel()
            self.activity_watcher = None

        self_deployment = self.apps_v1.read_namespaced_deployment(self.self_deployment_name, self.target_namespace)
        service = self.v1.read_namespaced_service(self.service_name, self.target_namespace)
        service.spec.selector = self_deployment.spec.selector.match_labels
        self.v1.patch_namespaced_service(self.service_name, self.target_namespace, service)
        log.info('Service %s updated', self.service_name)

        deployment = self.apps_v1.read_namespaced_deployment(self.target_deployment_name, self.target_namespace)
        deployment.spec.replicas = 0
        self.apps_v1.patch_namespaced_deployment(self.target_deployment_name, self.target_namespace, deployment)
        log.info('Deployment %s scaled', self.target_deployment_name)

        with ignore_404():
            self.v1.delete_namespaced_persistent_volume_claim(self.target_pvc_name, self.target_namespace)
            log.info('PVC %s deleted', self.target_pvc_name)

        self.state = State.DISENGAGED

    def list_snapshots(self, disk):
        combined = []
        page_token = None

        while True:
            response = self.compute.snapshots().list(
                project=self.gcp_project,
                filter=f'(sourceDisk = "{disk}")',
                pageToken=page_token,
            ).execute()

            if 'items' in response:
                combined += response['items']

            if 'nextPageToken' in response:
                page_token = response['nextPageToken']
            else:
                break

        return sorted(combined, key=lambda s: s['creationTimestamp'], reverse=True)

    def wait_for_operation(self, operation):
        while True:
            result = self.compute.zoneOperations().get(
                project=self.gcp_project,
                zone=self.gcp_zone,
                operation=operation,
            ).execute()

            if result['status'] == 'DONE':
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)


@contextlib.contextmanager
def ignore_404():
    try:
        yield
    except kubernetes.client.rest.ApiException as err:
        if err.status == 404:
            pass
        else:
            raise err
