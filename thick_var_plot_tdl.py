from plotly.subplots import make_subplots
def TDL_yield_chart():
    if os.path.exists('df-data-and-ai.json'):
        sm_credentials = service_account.Credentials.from_service_account_file('df-data-and-ai.json')
    else:
        sm_credentials, project = google.auth.default()

    # AWS SES SMTP Configuration
    smtp_server = 'email-smtp.us-west-2.amazonaws.com'
    smtp_port = 587
    svc_json_string, login, password = fetchSmtpCredentials(sm_credentials)
    print(json.loads(svc_json_string)["client_email"])  

    # GCP Client Config
    credentials = service_account.Credentials.from_service_account_info(json.loads(svc_json_string))
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket('df_ml_v_growth_events')
    query_client = bigquery.Client(credentials=credentials)

    ###########################
    ### TDL Post Split thickness distribution ###
    ###########################
    
    query_tdl_thickness_per_block = ''' 
with first_char as (select sample_id, min(data_collection_complete_date) as data_collection_complete_date 
        from `df-mes.mes_warehouse.data_collection`
        where data_collection_name = 'TDL/Despike FRT_CWL'
        and flow_name != 'TDL-TA_Master V0A'
        and data_collection_complete_date > current_timestamp() - interval 90 day
        group by 1
        ),
last_laser as (
  SELECT id_block, recipe_target
  FROM `df-data-and-ai.trilliant_warehouse.TDL_laser_recipe`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id_block
    ORDER BY timestamp DESC
  ) = 1
),
    raw as (
        select blocks.parent_id_block,dc.sample_id as id_block,nsl.data as layer, first_char.data_collection_complete_date,manufacturer_lot_number as batch,product_name,
          safe_cast(substr(tgt.recipe_target,1,3) as float64)  as target_thick,
        max(substr(tgt.recipe_target,1,3)) as recipe_target,
        max(dc.flow_name) as flow,
        max(if(parameter_name = 'min_thickness_despike_filtered', safe_cast(collected_value as float64), NULL)) as minThick_raw,
        max(if(parameter_name = 'max_thickness_despike_filtered', safe_cast(collected_value as float64), NULL)) as maxThick_raw
        from `df-mes.mes_warehouse.data_collection` dc
        join first_char on dc.sample_id = first_char.sample_id and dc.data_collection_complete_date < first_char.data_collection_complete_date + interval 10 minute
        join `df-max.raw_dfdb.blocks` blocks on dc.sample_id = blocks.id_block
        join `df-data-and-ai.trilliant_warehouse.New_seed_layer` nsl on dc.sample_id = nsl.id_block
        join last_laser tgt on tgt.id_block = blocks.parent_id_block
        where dc.data_collection_complete_date > current_timestamp() - interval 7 day
        and parent_id_block is not null
        group by 1,2,3,4,5,6,7
    ),
    scrapped as (
      select parent_id_block,id_block, nsl.data as layer, min(modified_on) as time_terminated
      from
        `df-mes.mes_warehouse.block_material_history` as l
        join `df-max.raw_dfdb.blocks` blocks using(id_block)
        join `df-data-and-ai.trilliant_warehouse.New_seed_layer` nsl using(id_block)
        where operation_name = 'Terminate'
        and modified_on > current_timestamp() - interval 90 day
        and parent_id_block in (select parent_id_block from raw)
        and substr(l.step_name,1,3) in ('304', '344', '704')
        group by 1,2,3
    ),
    raw_with_scrapped as (
      select 
      coalesce(raw.parent_id_block, scrapped.parent_id_block) as parent_id_block, 
      coalesce(raw.id_block,scrapped.id_block) as id_block, 
      coalesce(raw.layer, scrapped.layer) as layer, data_collection_complete_date,scrapped.time_terminated,
      batch,
      product_name,
      minThick_raw,
      maxThick_raw,
      target_thick,
      recipe_target,
      minThick_raw - target_thick as minThick,
      maxThick_raw - target_thick as maxThick
      from raw
      FULL OUTER JOIN scrapped using(parent_id_block, id_block)
    ),
    raw_binned as (
      select raw_with_scrapped.*, 
      least(pbd.width, pbd.length) as parent_size,
      case when least(pbd.width, pbd.length) > 25 then 'large' else 'small' end as size_group,
         DENSE_RANK() OVER (
          ORDER BY 
          case when least(pbd.width, pbd.length) > 25 then 'large' else 'small' end,
          parent_id_block
          ) as parent_rank,
      from raw_with_scrapped
        join `df-max.raw_dfdb.block_dimensions` pbd on raw_with_scrapped.parent_id_block = pbd.id_block
    )
    select *, 
    case when time_terminated is not null then 1 else 0 end as Terminated, 
    case 
        when maxThick > 80 and minThick < -80 then 'Both'
        when maxThick < -80 then 'AllMin'
        when minThick > 80 then 'AllMax'
        when maxThick > 80 then 'Max'
        when minThick < -80  then 'Min'
        else NULL end as outlier_type
    from raw_binned
    order by parent_id_block, layer, data_collection_complete_date
    '''
    df_query_tdl_thickness_per_block = query_client.query(query_tdl_thickness_per_block).result().to_dataframe()
    Available_Layers = list(df_query_tdl_thickness_per_block['layer'].unique())
    Available_Layers.sort()
    Available_Layers.insert(0,Available_Layers.pop()) # move the 'top' to the first element of this list eg ['top', 'C1', 'C2', 'C3', 'C4', 'bottom']

    df_query_tdl_thickness_per_block['minThick'] = pd.to_numeric(df_query_tdl_thickness_per_block['minThick'], errors='coerce')
    df_query_tdl_thickness_per_block['maxThick'] = pd.to_numeric(df_query_tdl_thickness_per_block['maxThick'], errors='coerce')
    
    df_query_tdl_thickness_per_block['parent_rank'] = pd.to_numeric(df_query_tdl_thickness_per_block['parent_rank'], errors='coerce')


    num_plots = len(Available_Layers)
    row_heights = [1/num_plots] * num_plots
    limit = 80
    tolerance = 50
    # Create subplots: stacked vertically, shared x-axis
    fig = make_subplots(
        rows=num_plots, cols=1,
        shared_xaxes=True,
        vertical_spacing=0,
        row_heights=row_heights
    )
    
    for i, layer in enumerate(Available_Layers):
        # filter the dataframe for this layer
        df_sub = df_query_tdl_thickness_per_block[df_query_tdl_thickness_per_block['layer'] == layer]
        # plot each vertical "box"
        for _, row in df_sub.iterrows():
            fig.add_trace(
                go.Scatter(
                    x=[row['parent_rank'], row['parent_rank']],
                    y=[row['minThick'], row['maxThick']],
                    mode='lines',
                    line=dict(color='#6fa8dc', width=400/df_query_tdl_thickness_per_block['parent_rank'].max()),
                    showlegend=False
                ),
                row=i+1, col=1
            )
            if pd.notna(row['outlier_type']) :
                if row['outlier_type'] in ['Both', 'Max']:
                    fig.add_trace(
                        go.Scatter(
                            x=[row['parent_rank'], row['parent_rank']],
                            y=[limit*0.9,limit],
                            mode='lines',
                            line=dict(color='red', width=400/df_query_tdl_thickness_per_block['parent_rank'].max()),
                            showlegend=False
                        ),
                        row=i+1, col=1
                    )
                    # print(row['parent_rank'],  ' upper')
                if row['outlier_type'] in ['Both', 'Min']:
                    fig.add_trace(
                        go.Scatter(
                            x=[row['parent_rank'], row['parent_rank']],
                            y=[-limit,-limit*0.9],
                            mode='lines',
                            line=dict(color='red', width=400/df_query_tdl_thickness_per_block['parent_rank'].max()),
                            showlegend=False
                        ),
                        row=i+1, col=1
                    )
                ####both max and min out of range
                ## Plot as full blocks
                # if row['outlier_type'] in [ 'AllMin']:
                #     fig.add_trace(
                #         go.Scatter(
                #             x=[row['parent_rank'], row['parent_rank']],
                #             y=[0-limit,0],
                #             mode='lines',
                #             line=dict(color='red', width=400/df_query_tdl_thickness_per_block['parent_rank'].max()),
                #             showlegend=False
                #         ),
                #         row=i+1, col=1
                #     )
                # if row['outlier_type'] in ['AllMax']:
                #     fig.add_trace(
                #         go.Scatter(
                #             x=[row['parent_rank'], row['parent_rank']],
                #             y=[0,limit],
                #             mode='lines',
                #             line=dict(color='red', width=400/df_query_tdl_thickness_per_block['parent_rank'].max()),
                #             showlegend=False
                #         ),
                #         row=i+1, col=1
                #     )
                ## plot as arrow
                if row['outlier_type'] in ['AllMin']:
                    fig.add_annotation(
                        ax=row['parent_rank'],  # arrow start (same x)
                        ay=limit*0.5,    
                        x=row['parent_rank'],   # arrow tip (target)
                        y=-limit,          
                        xref=f'x{i+1}',
                        yref=f'y{i+1}',
                        axref=f'x{i+1}',
                        ayref=f'y{i+1}',
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=1.2,
                        arrowcolor='red'
                    )
                if row['outlier_type'] in ['AllMax']:
                    fig.add_annotation(
                        ax=row['parent_rank'],  # arrow start (same x)
                        ay=-limit*0.5,    
                        x=row['parent_rank'],   # arrow tip (target)
                        y=limit,          
                        xref=f'x{i+1}',
                        yref=f'y{i+1}',
                        axref=f'x{i+1}',
                        ayref=f'y{i+1}',
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=1.2,
                        arrowcolor='red'
                    )
        
        # add reference line for large plate
        fig.add_shape(
            type="line",
            x0= df_query_tdl_thickness_per_block['parent_rank'].min()-0.5,
            x1= df_query_tdl_thickness_per_block['parent_rank'].max()+0.5, # reference line should go for the same range
            y0=0,
            y1=0,
            line=dict(color="#1c4587", width=1),
            row=i+1, col=1
        )

        # add an annotation
        fig.add_annotation(
            x=df_query_tdl_thickness_per_block['parent_rank'].max() + 0.5,
            y=0,
            text="Spec Thk",
            showarrow=False,
            xanchor="left",
            yanchor="middle",
            font=dict(color="#1c4587", size=10),
            row=i+1,
            col=1
        )
        
        # Add X for scrapped plates
        df_scrap = df_sub[df_sub['Terminated']==1]
        fig.add_trace(
            go.Scatter(
                x=df_scrap['parent_rank'],
                y=[0] * len(df_scrap),
                mode='markers',
                marker=dict(
                    symbol='x-thin-open',
                    size=10,
                    color='black'
                ),
                showlegend=False
            ),
            row=i+1,
            col=1
        )
        
        # customize y-axis per subplot
        y_lines = [ 0 + tolerance, 0 - tolerance]
        
        fig.update_yaxes(
            showgrid = False,
            showticklabels = False,
            # tickvals=y_lines,
            ticktext=[str(v) for v in y_lines],
            title_text=f"{layer}",
            range=[
                -limit, #min(df_sub.minThick.min(), - tolerance*1.1),
                limit #max(df_sub.maxThick.max(), + tolerance*1.1) # different y range per charlottee
            ],
            row=i+1,
            col=1
        )
        
        for y in y_lines:
            fig.add_shape(
                type="line",
                x0= df_query_tdl_thickness_per_block['parent_rank'].min()-0.5,
                x1= df_query_tdl_thickness_per_block['parent_rank'].max()+0.5,
                y0=y,
                y1=y,
                line=dict(color="#cccccc", width=1, dash="dot"),
                row=i+1,
                col=1
            )

        # For troubleshoot only
        if layer == Available_Layers[-1]:
            fig.update_xaxes(
                title_text="Parent Block",           # x-axis label
                showticklabels=True,
                tickvals=df_query_tdl_thickness_per_block['parent_rank'],
                ticktext=df_query_tdl_thickness_per_block['parent_id_block'],
                range=[
                    df_query_tdl_thickness_per_block['parent_rank'].min() - 0.5,
                    df_query_tdl_thickness_per_block['parent_rank'].max() + 0.5
                ],
                row=i+1,
                col=1
            )
            
    # fig.update_xaxes(showticklabels=False)
    
    # layout adjustments
    fig.update_layout(
        plot_bgcolor="white",
        xaxis=dict(
            range=[
                df_query_tdl_thickness_per_block['parent_rank'].min()-0.5,
                df_query_tdl_thickness_per_block['parent_rank'].max()+0.5
            ]
        ),
        height=600,
        width = 800
    )
    
    fig.show()
    return df_query_tdl_thickness_per_block
