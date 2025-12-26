// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package fabricplugin

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Azure/azqr/internal/models"
	"github.com/Azure/azqr/internal/plugins"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/microsoft/fabric-sdk-go/fabric"
	"github.com/microsoft/fabric-sdk-go/fabric/core"
	"github.com/rs/zerolog/log"
)

// FabricDetailsScanner is an internal plugin that scans Fabric capacity details
type FabricDetailsScanner struct{}

// NewFabricDetailsScanner creates a new fabric details scanner
func NewFabricDetailsScanner() *FabricDetailsScanner {
	return &FabricDetailsScanner{}
}

// GetMetadata returns plugin metadata
func (s *FabricDetailsScanner) GetMetadata() plugins.PluginMetadata {
	return plugins.PluginMetadata{
		Name:        "fabric-details",
		Version:     "1.0.0",
		Description: "Analyzes Microsoft Fabric capacity workspaces and items",
		Author:      "Azure Quick Review Team",
		License:     "MIT",
		Type:        plugins.PluginTypeInternal,
		ColumnMetadata: []plugins.ColumnMetadata{
			{Name: "Capacity ID", DataKey: "capacityId", FilterType: plugins.FilterTypeDropdown},
			{Name: "Workspace Count", DataKey: "workspaceCount", FilterType: plugins.FilterTypeNone},
			{Name: "Total Items", DataKey: "totalItems", FilterType: plugins.FilterTypeNone},
			{Name: "Lakehouses", DataKey: "lakehouses", FilterType: plugins.FilterTypeNone},
			{Name: "Notebooks", DataKey: "notebooks", FilterType: plugins.FilterTypeNone},
			{Name: "Reports", DataKey: "reports", FilterType: plugins.FilterTypeNone},
			{Name: "Semantic Models", DataKey: "semanticModels", FilterType: plugins.FilterTypeNone},
			{Name: "Warehouses", DataKey: "warehouses", FilterType: plugins.FilterTypeNone},
			{Name: "Pipelines", DataKey: "pipelines", FilterType: plugins.FilterTypeNone},
			{Name: "Other Items", DataKey: "otherItems", FilterType: plugins.FilterTypeNone},
			{Name: "Workspaces", DataKey: "workspaces", FilterType: plugins.FilterTypeSearch},
		},
	}
}

// WorkspaceInfo contains workspace details from Fabric API
type WorkspaceInfo struct {
	ID          string
	Name        string
	CapacityID  string
	ItemCounts  map[string]int
	TotalItems  int
}

// CapacityDetails contains aggregated data for a capacity
type CapacityDetails struct {
	CapacityID     string
	WorkspaceCount int
	TotalItems     int
	ItemBreakdown  map[string]int
	Workspaces     []string
}

// Scan executes the plugin and returns table data
func (s *FabricDetailsScanner) Scan(ctx context.Context, cred azcore.TokenCredential, subscriptions map[string]string, filters *models.Filters) (*plugins.ExternalPluginOutput, error) {
	log.Info().Msg("Scanning Fabric capacity details")

	// Create Fabric API client
	fabClient, err := fabric.NewClient(cred, nil, nil)
	if err != nil {
		log.Warn().Err(err).Msg("Could not create Fabric API client - Fabric API may not be accessible")
		return s.emptyOutput(), nil
	}

	coreCF := core.NewClientFactoryWithClient(*fabClient)
	workspacesClient := coreCF.NewWorkspacesClient()
	itemsClient := coreCF.NewItemsClient()

	// Get workspaces
	workspaces, err := s.getWorkspaces(ctx, workspacesClient)
	if err != nil {
		log.Warn().Err(err).Msg("Could not retrieve workspaces from Fabric API")
		return s.emptyOutput(), nil
	}

	// Get items for each workspace
	for i := range workspaces {
		itemCounts, totalItems, err := s.getWorkspaceItems(ctx, itemsClient, workspaces[i].ID)
		if err != nil {
			log.Debug().Msgf("Could not get items for workspace %s: %v", workspaces[i].Name, err)
			continue
		}
		workspaces[i].ItemCounts = itemCounts
		workspaces[i].TotalItems = totalItems
	}

	// Aggregate by capacity
	capacities := s.aggregateByCapacity(workspaces)

	// Build table
	table := s.buildTable(capacities)

	log.Info().Msgf("Fabric details scan completed with %d capacities and %d workspaces", len(capacities), len(workspaces))

	return &plugins.ExternalPluginOutput{
		Metadata:    s.GetMetadata(),
		SheetName:   "Fabric Details",
		Description: "Microsoft Fabric capacity workspaces and items analysis",
		Table:       table,
	}, nil
}

func (s *FabricDetailsScanner) emptyOutput() *plugins.ExternalPluginOutput {
	return &plugins.ExternalPluginOutput{
		Metadata:    s.GetMetadata(),
		SheetName:   "Fabric Details",
		Description: "Microsoft Fabric capacity workspaces and items analysis",
		Table: [][]string{
			{"Capacity ID", "Workspace Count", "Total Items", "Lakehouses", "Notebooks", "Reports", "Semantic Models", "Warehouses", "Pipelines", "Other Items", "Workspaces"},
		},
	}
}

func (s *FabricDetailsScanner) getWorkspaces(ctx context.Context, client *core.WorkspacesClient) ([]WorkspaceInfo, error) {
	log.Info().Msg("Fetching workspaces from Fabric API")

	var workspaces []WorkspaceInfo
	pager := client.NewListWorkspacesPager(nil)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, ws := range page.Value {
			wsInfo := WorkspaceInfo{
				ID:         *ws.ID,
				Name:       *ws.DisplayName,
				ItemCounts: make(map[string]int),
			}

			if ws.CapacityID != nil {
				wsInfo.CapacityID = *ws.CapacityID
			}

			workspaces = append(workspaces, wsInfo)
		}
	}

	log.Info().Msgf("Found %d workspaces", len(workspaces))
	return workspaces, nil
}

func (s *FabricDetailsScanner) getWorkspaceItems(ctx context.Context, client *core.ItemsClient, workspaceID string) (map[string]int, int, error) {
	itemCounts := make(map[string]int)
	totalItems := 0

	pager := client.NewListItemsPager(workspaceID, nil)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return itemCounts, 0, err
		}

		for _, item := range page.Value {
			if item.Type != nil {
				itemType := string(*item.Type)
				itemCounts[itemType]++
				totalItems++
			}
		}
	}

	return itemCounts, totalItems, nil
}

func (s *FabricDetailsScanner) aggregateByCapacity(workspaces []WorkspaceInfo) map[string]*CapacityDetails {
	capacities := make(map[string]*CapacityDetails)

	for _, ws := range workspaces {
		if ws.CapacityID == "" {
			continue
		}

		if capacities[ws.CapacityID] == nil {
			capacities[ws.CapacityID] = &CapacityDetails{
				CapacityID:    ws.CapacityID,
				ItemBreakdown: make(map[string]int),
			}
		}

		cap := capacities[ws.CapacityID]
		cap.WorkspaceCount++
		cap.TotalItems += ws.TotalItems
		cap.Workspaces = append(cap.Workspaces, ws.Name)

		for itemType, count := range ws.ItemCounts {
			cap.ItemBreakdown[itemType] += count
		}
	}

	return capacities
}

func (s *FabricDetailsScanner) buildTable(capacities map[string]*CapacityDetails) [][]string {
	// Initialize table with headers
	table := [][]string{
		{"Capacity ID", "Workspace Count", "Total Items", "Lakehouses", "Notebooks", "Reports", "Semantic Models", "Warehouses", "Pipelines", "Other Items", "Workspaces"},
	}

	// Sort capacity IDs for consistent output
	capIDs := make([]string, 0, len(capacities))
	for capID := range capacities {
		capIDs = append(capIDs, capID)
	}
	sort.Strings(capIDs)

	// Convert to table rows
	for _, capID := range capIDs {
		cap := capacities[capID]

		// Extract common item types
		lakehouses := cap.ItemBreakdown["Lakehouse"]
		notebooks := cap.ItemBreakdown["Notebook"]
		reports := cap.ItemBreakdown["Report"]
		semanticModels := cap.ItemBreakdown["SemanticModel"]
		warehouses := cap.ItemBreakdown["Warehouse"]
		pipelines := cap.ItemBreakdown["DataPipeline"]

		// Calculate other items
		knownItems := lakehouses + notebooks + reports + semanticModels + warehouses + pipelines
		otherItems := cap.TotalItems - knownItems

		row := []string{
			capID,
			strconv.Itoa(cap.WorkspaceCount),
			strconv.Itoa(cap.TotalItems),
			strconv.Itoa(lakehouses),
			strconv.Itoa(notebooks),
			strconv.Itoa(reports),
			strconv.Itoa(semanticModels),
			strconv.Itoa(warehouses),
			strconv.Itoa(pipelines),
			strconv.Itoa(otherItems),
			strings.Join(cap.Workspaces, ", "),
		}

		table = append(table, row)
	}

	return table
}

// Ensure interface compliance
var _ plugins.InternalPluginScanner = (*FabricDetailsScanner)(nil)

// Suppress unused import warning
var _ = fmt.Sprintf

// init registers the plugin automatically
func init() {
	plugins.RegisterInternalPlugin("fabric-details", NewFabricDetailsScanner())
}
