/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scaleway

import (
	"fmt"
	"os"

	block "github.com/scaleway/scaleway-sdk-go/api/block/v1alpha1"
	"github.com/scaleway/scaleway-sdk-go/api/instance/v1"
	ipam "github.com/scaleway/scaleway-sdk-go/api/ipam/v1"
	"github.com/scaleway/scaleway-sdk-go/scw"
	"k8s.io/klog/v2"
	"sigs.k8s.io/etcd-manager/pkg/volumes"
)

const (
	sbsDevicePrefix                   = "/dev/disk/by-id/scsi-0SCW_sbs_volume-"
	productResourceTypeInstanceServer = "instance_server"
)

// Volumes defines the Scaleway Cloud volume implementation.
type Volumes struct {
	clusterName string
	matchTags   []string
	nameTag     string

	scwClient   *scw.Client
	server      *instance.Server
	zone        scw.Zone
	instanceAPI *instance.API
	blockAPI    *block.API
}

var _ volumes.Volumes = &Volumes{}

// NewVolumes returns a new Scaleway Cloud volume provider.
func NewVolumes(clusterName string, volumeTags []string, nameTag string) (*Volumes, error) {
	scwClient, err := scw.NewClient(
		scw.WithEnv(),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating Scaleway client: %w", err)
	}

	metadataAPI := instance.NewMetadataAPI()
	metadata, err := metadataAPI.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve server metadata: %w", err)
	}

	serverID := metadata.ID
	klog.V(2).Infof("Found ID of the running server: %s", serverID)

	zoneID := metadata.Location.ZoneID
	zone, err := scw.ParseZone(zoneID)
	if err != nil {
		return nil, fmt.Errorf("unable to parse Scaleway zone: %w", err)
	}
	klog.V(2).Infof("Found zone of the running server: %v", zone)

	instanceAPI := instance.NewAPI(scwClient)
	server, err := instanceAPI.GetServer(&instance.GetServerRequest{
		ServerID: serverID,
		Zone:     zone,
	})
	if err != nil || server == nil || server.Server == nil {
		return nil, fmt.Errorf("failed to get the running server: %w", err)
	}
	klog.V(2).Infof("Found the running server: %q", server.Server.Name)

	a := &Volumes{
		clusterName: clusterName,
		matchTags:   volumeTags,
		nameTag:     nameTag,
		scwClient:   scwClient,
		server:      server.Server,
		zone:        zone,
		instanceAPI: instanceAPI,
		blockAPI:    block.NewAPI(scwClient),
	}

	return a, nil
}

// FindVolumes returns all volumes that can be attached to the running server.
func (a *Volumes) FindVolumes() ([]*volumes.Volume, error) {
	klog.V(2).Infof("Finding attachable etcd volumes")

	matchingEtcdVolumes, err := a.getMatchingBlockVolumes(append(a.matchTags, a.nameTag))
	if err != nil {
		return nil, fmt.Errorf("failed to get matching volumes: %w", err)
	}

	var localEtcdVolumes []*volumes.Volume
	for _, volume := range matchingEtcdVolumes {
		if volume.Zone == "" {
			klog.Warningf("failed to find volume location for %s(%s)", volume.Name, volume.ID)
			continue
		}
		if volume.Zone != a.zone {
			continue
		}
		klog.V(2).Infof("Found attachable volume %s(%s) of type %s with status %q", volume.Name, volume.ID, volume.Type, volume.Status)

		localEtcdVolume := &volumes.Volume{
			ProviderID: volume.ID,
			Info: volumes.VolumeInfo{
				Description: a.clusterName + "-" + volume.ID,
			},
			MountName: "scw-" + volume.ID,
			EtcdName:  "vol-" + volume.ID,
		}

		// Check if the volume is attached to a server via its references
		for _, ref := range volume.References {
			if ref.ProductResourceType == productResourceTypeInstanceServer {
				localEtcdVolume.AttachedTo = ref.ProductResourceID
				if ref.ProductResourceID == a.server.ID {
					localEtcdVolume.LocalDevice = fmt.Sprintf("%s%s", sbsDevicePrefix, volume.ID)
				}
				break
			}
		}

		localEtcdVolumes = append(localEtcdVolumes, localEtcdVolume)
	}

	return localEtcdVolumes, nil
}

// FindMountedVolume returns the device where the volume is mounted to the running server.
func (a *Volumes) FindMountedVolume(volume *volumes.Volume) (string, error) {
	device := volume.LocalDevice

	klog.V(2).Infof("Finding mounted volume %q", device)
	_, err := os.Stat(volumes.PathFor(device))
	if err == nil {
		klog.V(2).Infof("Found mounted volume %q", device)
		return device, nil
	}

	if !os.IsNotExist(err) {
		return "", fmt.Errorf("failed to find local device %q: %w", device, err)
	}

	// When not found, the interface says to return ("", nil)
	return "", nil
}

// AttachVolume attaches the specified volume to the running server and returns the mountpoint if successful.
func (a *Volumes) AttachVolume(volume *volumes.Volume) error {
	blockVolume, err := a.blockAPI.GetVolume(&block.GetVolumeRequest{
		VolumeID: volume.ProviderID,
		Zone:     a.zone,
	})
	if err != nil {
		return fmt.Errorf("failed to get info for volume id %q: %w", volume.ProviderID, err)
	}

	// Check if volume is already attached via its references
	for _, ref := range blockVolume.References {
		if ref.ProductResourceType == productResourceTypeInstanceServer {
			if ref.ProductResourceID != a.server.ID {
				return fmt.Errorf("found volume %s(%s) attached to a different server: %s", blockVolume.Name, blockVolume.ID, ref.ProductResourceID)
			}
			klog.V(2).Infof("Volume %s(%s) is already attached to the running server", blockVolume.Name, blockVolume.ID)
			volume.LocalDevice = fmt.Sprintf("%s%s", sbsDevicePrefix, blockVolume.ID)
			return nil
		}
	}

	// Attach the SBS volume to the server using the Instance API
	klog.V(2).Infof("Attaching SBS volume %s(%s) to the running server", blockVolume.Name, blockVolume.ID)
	_, err = a.instanceAPI.AttachServerVolume(&instance.AttachServerVolumeRequest{
		Zone:       a.zone,
		ServerID:   a.server.ID,
		VolumeID:   blockVolume.ID,
		VolumeType: instance.AttachServerVolumeRequestVolumeTypeSbsVolume,
	})
	if err != nil {
		return fmt.Errorf("failed to attach volume %s(%s): %w", blockVolume.Name, blockVolume.ID, err)
	}

	// Wait for the volume and its references to be in a stable state
	_, err = a.blockAPI.WaitForVolumeAndReferences(&block.WaitForVolumeAndReferencesRequest{
		VolumeID: blockVolume.ID,
		Zone:     a.zone,
	})
	if err != nil {
		return fmt.Errorf("error waiting for volume %s(%s): %w", blockVolume.Name, blockVolume.ID, err)
	}
	_, err = a.instanceAPI.WaitForServer(&instance.WaitForServerRequest{
		ServerID: a.server.ID,
		Zone:     a.zone,
	})
	if err != nil {
		return fmt.Errorf("error waiting for server %s(%s): %w", a.server.Name, a.server.ID, err)
	}

	volume.LocalDevice = fmt.Sprintf("%s%s", sbsDevicePrefix, blockVolume.ID)
	return nil
}

// MyIP returns the first private IP of the running server if successful.
func (a *Volumes) MyIP() (string, error) {
	ip, err := a.getServerIP(a.server.ID)
	if err != nil {
		return "", fmt.Errorf("getting IP for server %s: %w", a.server.ID, err)
	}
	klog.V(2).Infof("Found first private IP of the running server: %s", ip)
	return ip, nil
}

// getMatchingBlockVolumes returns all block volumes matching matchTags.
func (a *Volumes) getMatchingBlockVolumes(matchTags []string) ([]*block.Volume, error) {
	resp, err := a.blockAPI.ListVolumes(&block.ListVolumesRequest{
		Zone: a.zone,
		Tags: matchTags,
	}, scw.WithAllPages())
	if err != nil {
		return nil, fmt.Errorf("failed to get block volumes matching tags %q: %w", matchTags, err)
	}

	// The Block API may not filter by all tags (AND). Filter client-side.
	var matched []*block.Volume
	for _, vol := range resp.Volumes {
		if hasAllTags(vol.Tags, matchTags) {
			matched = append(matched, vol)
		}
	}
	klog.V(6).Infof("Got %d block volumes from API, %d matched all tags %v", resp.TotalCount, len(matched), matchTags)
	return matched, nil
}

func hasAllTags(volumeTags []string, requiredTags []string) bool {
	tagSet := make(map[string]bool, len(volumeTags))
	for _, t := range volumeTags {
		tagSet[t] = true
	}
	for _, t := range requiredTags {
		if !tagSet[t] {
			return false
		}
	}
	return true
}

func (a *Volumes) getServerIP(serverID string) (string, error) {
	server, err := a.instanceAPI.GetServer(&instance.GetServerRequest{
		ServerID: serverID,
		Zone:     a.zone,
	})
	if err != nil || server == nil || server.Server == nil {
		return "", fmt.Errorf("getting server %s: %w", serverID, err)
	}

	// Prefer private IP from Instance API (legacy VPC)
	if server.Server.PrivateIP != nil && *server.Server.PrivateIP != "" {
		return *server.Server.PrivateIP, nil
	}

	// Fall back to public IPs
	for _, ip := range server.Server.PublicIPs {
		if ip != nil && ip.Address != nil {
			return ip.Address.String(), nil
		}
	}

	// Fall back to IPAM for private-network-only instances.
	region, err := a.zone.Region()
	if err != nil {
		return "", fmt.Errorf("no IP found for server %s (unable to parse region: %w)", serverID, err)
	}
	ipamAPI := ipam.NewAPI(a.scwClient)
	for _, nic := range server.Server.PrivateNics {
		nicIPs, err := ipamAPI.ListIPs(&ipam.ListIPsRequest{
			Region:           region,
			PrivateNetworkID: scw.StringPtr(nic.PrivateNetworkID),
			ResourceID:       scw.StringPtr(nic.ID),
			ResourceType:     ipam.ResourceTypeInstancePrivateNic,
			IsIPv6:           scw.BoolPtr(false),
		}, scw.WithAllPages())
		if err != nil {
			klog.Warningf("getServerIP: IPAM query for NIC %s failed: %v", nic.ID, err)
			continue
		}
		if nicIPs.TotalCount > 0 && len(nicIPs.IPs[0].Address.IP) > 0 {
			return nicIPs.IPs[0].Address.IP.String(), nil
		}
	}

	return "", fmt.Errorf("no IP found for server %s", serverID)
}
