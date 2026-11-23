package main

import (
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/catalog"
	"github.com/coffeebreak-ai/coffee-opencode-llm/internal/store"
)

// catalogRows maps domain models into plain storage rows. The catalog loader
// owns the direction; the store stays domain-agnostic.
func catalogRows(models []catalog.Model) []store.CatalogModel {
	rows := make([]store.CatalogModel, 0, len(models))
	for _, model := range models {
		rows = append(rows, store.CatalogModel{
			ID:             model.ID,
			ContextTokens:  int64(model.ContextTokens),
			MaxOutput:      int64(model.MaxOutput),
			CostInPerMTok:  model.CostInPerMTok,
			CostOutPerMTok: model.CostOutPerMTok,
			QualityTier:    model.QualityTier,
			Retention:      model.Retention,
			QuotaGroup:     model.Quota.Group,
			Strengths:      model.Strengths,
			Limits:         model.Limits,
			Enabled:        model.Enabled,
			Roles:          model.Roles,
			Efforts:        model.Effort,
			Modalities:     model.Modalities,
			Features:       model.Features,
			Groups:         model.GroupSources,
		})
	}
	return rows
}
