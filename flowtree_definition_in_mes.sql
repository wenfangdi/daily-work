WITH subs AS (SELECT sf.SourceEntityId SourceId,
							sff.DefinitionId TargetId,
							CAST(REPLICATE('0',3-LEN(RTRIM(sf.Position))) + RTRIM(sf.Position) AS VARCHAR) Position,
							sff.Name FlowName,
							CAST(CONCAT(sffcv.Name,' [',sffcv.Revision,'.',sffcv.Version,'] (',sffcv.Description,')')AS NVARCHAR(1000)) PrettyFlowName,
							CAST(sffcv.Description AS NVARCHAR(1000)) PrettyFlowChain,
							CAST(CONCAT(sffcv.Name,' (',sffcv.Description,')')AS NVARCHAR(1000)) PrettyPath,
							sffcv.IsAlternate,
							CONCAT('/',sffcv.Name,':',sffcv.Revision+':',sf.CorrelationID) FlowPath,
							sf.CorrelationID,
							sf.LogicalName,
							0 DisplayOrder
				FROM CoreDataModel.T_SubFlow sf
				INNER JOIN df.UniversalState sfus ON sfus.UniversalStateId=sf.UniversalState
				INNER JOIN CoreDataModel.T_Flow sff ON sf.TargetEntityId=sff.FlowId
				INNER JOIN CoreDataModel.T_Flow sffcv ON sffcv.FlowId=sff.DefinitionId
				AND sfus.Name='Active'),
	FlowTree AS(SELECT f.FlowId RootFlowId,
						f.Name RootFlowName,
						f.DefinitionId SourceId,
						f.DefinitionId TargetId,
						f.Name FlowName,
						CAST(CONCAT(fcv.Name,' [',fcv.Revision,'.',fcv.Version,'] (',fcv.Description,')')AS NVARCHAR(1000)) PrettyFlowName,
						CAST(fcv.Description AS NVARCHAR(1000)) PrettyFlowChain,
						CAST(CONCAT(f.Name,' (',fcv.Description,')') AS NVARCHAR(1000)) PrettyPath,
						CAST(CONCAT(f.Name,':',fcv.Revision,':','1') AS NVARCHAR(1000)) FlowPath,
						CAST('001' AS NVARCHAR(1000)) Position,
						f.IsAlternate IsAlternate,
						1 Level,
						CAST(1 AS BIGINT) CorrelationID,
						CAST('' AS NVARCHAR(256)) LogicalName,
						0 Displayorder
					FROM CoreDataModel.T_Flow f
					INNER JOIN CoreDataModel.T_Flow fcv ON fcv.FlowId=f.DefinitionId
					WHERE f.FlowId=@ThisFlow
				UNION ALL SELECT FlowTree.RootFlowId,
					FlowTree.RootFlowName,
					s2.SourceId,
					s2.TargetId,
					s2.FlowName FlowName,
					s2.PrettyFlowName,
					CAST(CONCAT(FlowTree.PrettyFlowChain,'-->',s2.PrettyFlowChain) AS NVARCHAR(1000)) PrettyFlowChain,
					CAST(CONCAT(FlowTree.PrettyPath,CHAR(13),s2.PrettyPath) AS NVARCHAR(1000)) PrettyPath,
					CAST(CONCAT(FlowTree.FlowPath,s2.FlowPath) AS NVARCHAR(1000)) FlowPath,
					CAST(CONCAT(FlowTree.Position,s2.Position) AS NVARCHAR(1000)) Position,
					s2.IsAlternate,
					FlowTree.level+1 level,
					s2.CorrelationID,
					s2.LogicalName,
					s2.DisplayOrder
					FROM subs s2
					INNER JOIN FlowTree ON s2.SourceId=FlowTree.TargetId),
	StepBranches AS (SELECT FlowTree.RootFlowId,
							FlowTree.RootFlowName,
							FlowTree.SourceId,
							FlowTree.TargetId,
							FlowTree.FlowName,
							FlowTree.IsAlternate,
							FlowTree.PrettyFlowName,
							FlowTree.PrettyFlowChain,
							CONCAT(FlowTree.FlowPath,'/',s.Name,':',fs.CorrelationID) FlowPath,
							CONCAT(FlowTree.PrettyPath,CHAR(13),s.Name,' (',COALESCE(fs.LogicalName,s.Description),')') PrettyPath,
							FlowTree.Level+1 Level,
							CONCAT(FlowTree.Position,REPLICATE('0',3-LEN(RTRIM(fs.Position))), RTRIM(fs.Position)) Position,
							FlowTree.CorrelationID,
							s.Name StepName,
							CONCAT(s.Name,' (',COALESCE(fs.LogicalName,s.Description),')') PrettyStep,
							s.StepId,
							MAX(Level) OVER () MaxLevel,
							fs.LogicalName LogicalName,
							s.DisplayOrder
						FROM FlowTree
						INNER JOIN CoreDataModel.T_FlowStep fs ON fs.SourceEntityId=FlowTree.TargetId
						INNER JOIN df.UniversalState fsus ON fsus.UniversalStateId=fs.UniversalState
						INNER JOIN CoreDataModel.T_Step s ON s.StepId=fs.TargetEntityId
						WHERE fsus.Name='Active'
					UNION ALL SELECT t.RootFlowId,
							t.RootFlowName,
							t.SourceId,
							t.TargetId,
							t.FlowName,
							t.IsAlternate,
							t.PrettyFlowName,
							t.PrettyFlowChain,
							t.FlowPath,
							t.PrettyPath,
							t.Level,
							t.Position,
							t.CorrelationID,
							NULL,
							NULL,
							NULL,
							MAX(Level+1) OVER () MaxLevel,
							t.LogicalName,
							0 DisplayOrder
					FROM FlowTree t)
SELECT b.RootFlowId,
		b.RootFlowName,
		b.SourceId FlowId,
		b.FlowName,
		b.IsAlternate,
		b.PrettyFlowName,
		b.PrettyFlowChain,
		b.PrettyPath,
		b.FlowPath,
		b.StepId,
		b.StepName,
		b.PrettyStep,
		b.Level,
		CAST(b.Position+REPLICATE('0',3*((MAX(b.MaxLevel) OVER ())-b.Level))AS NVARCHAR) TreePosition,
		b.LogicalName,
		b.DisplayOrder
	FROM StepBranches b
GO


