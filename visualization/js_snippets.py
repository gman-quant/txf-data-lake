# visualization/js_snippets.py

def get_legend_shorthand_fix(chart_id: str) -> str:
    """
    [BugFix 1] 繞過 lightweight-charts-python 內部在 sync=True 時的 Legend 同步崩潰 Bug，並關閉 K/M 縮寫
    """
    return f'''
    if ({chart_id}.legend) {{
        const orig = {chart_id}.legend.legendHandler.bind({chart_id}.legend);
        {chart_id}.legend.legendHandler = function(p, s) {{
            if (s && !p.seriesData) return;
            orig(p, s);
        }};
        // 覆寫內建的縮寫邏輯 (K/M)，強制顯示完整原始數值
        {chart_id}.legend.shorthandFormat = function(t) {{ return t.toString(); }};
    }}
    '''

def get_crosshair_sync_fix(main_chart_id: str, subchart_id: str, subchart_series_id: str) -> str:
    """
    [BugFix 2] 攔截 setCrosshairPosition 避免 Value is null 崩潰 (包含主副圖的雙向修復)
    """
    return f'''
    // 1. 修復 主圖 -> 副圖 的十字線同步 (攔截崩潰)
    const orig_set_sub = {subchart_id}.chart.setCrosshairPosition.bind({subchart_id}.chart);
    {subchart_id}.chart.setCrosshairPosition = function(price, time, series) {{
        if (!series) {{
            series = {subchart_series_id}.series;
        }}
        try {{
            if (series) orig_set_sub(price, time, series);
        }} catch(e) {{}}
    }};
    
    // 2. 修復 副圖 -> 主圖 的十字線同步 (攔截崩潰)
    const orig_set_main = {main_chart_id}.chart.setCrosshairPosition.bind({main_chart_id}.chart);
    {main_chart_id}.chart.setCrosshairPosition = function(price, time, series) {{
        if (!series) {{
            series = {main_chart_id}.series || ({main_chart_id}.lines.length > 0 ? {main_chart_id}.lines[0].series : null);
        }}
        try {{
            if (series) orig_set_main(price, time, series);
        }} catch(e) {{}}
    }};
    
    // 3. 補足套件殘缺的「副圖反向連動主圖」功能
    {subchart_id}.chart.subscribeCrosshairMove(param => {{
        if (!param.time) return;
        let targetSeries = {main_chart_id}.series || ({main_chart_id}.lines && {main_chart_id}.lines.length > 0 ? {main_chart_id}.lines[0].series : null);
        if (targetSeries) {{
            try {{
                // 給 0 作為 price 讓主圖只畫出垂直線 (因 0 超出主圖視角)
                {main_chart_id}.chart.setCrosshairPosition(0, param.time, targetSeries);
            }} catch(e) {{}}
        }}
    }});
    '''
