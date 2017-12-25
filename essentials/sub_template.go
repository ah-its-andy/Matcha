package essentials

import (
	"database/sql"
	"errors"
)

type SubTemplate struct {
	ID                 string
	Name               string
	Description        string
	CreationTime       int64
	CreationTimeString string
}

func (s *SubTemplate) Details(executor DbExecutor) ([]*SubTemplateDetails, error) {
	if s.ID == "" {
		return nil, WrapError("SubTemplate", errors.New("field ID is required"))
	}
	rows, err := executor.QueryScript("FetchSubTemplateDetails", s.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]*SubTemplateDetails, 0)
	for rows.Next() {
		var item SubTemplateDetails
		err = rows.Scan(&item.ID, &item.TemplateID, &item.ReceiverTag, &item.Exchange, &item.RouteKey, &item.CreationTime, &item.CreationTimeString)
		if err != nil {
			return nil, err
		}
		results = append(results, &item)
	}
	return results, nil
}

type SubTemplateDetails struct {
	ID                 string
	TemplateID         string
	ReceiverTag        string
	Exchange           string
	RouteKey           string
	CreationTime       int64
	CreationTimeString string
}

func FindOneTemplate(id string, name string, executor DbExecutor) (*SubTemplate, error) {
	row, err := executor.QueryScriptRow("FindOneTemplate", id, name)
	if err != nil {
		return nil, err
	}

	var template SubTemplate
	err = row.Scan(&template.ID, template.Name, template.Description, template.CreationTime, template.CreationTimeString)
	if sql.ErrNoRows == err {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &template, nil
}
