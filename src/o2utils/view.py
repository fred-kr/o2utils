import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots


def plot_dataset(
    df: pl.DataFrame,
    x_name: str,
    y_name: str,
    y2_name: str | None = None,
) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    x = df.get_column(x_name)
    y = df.get_column(y_name)
    y2 = df.get_column(y2_name) if y2_name is not None else None

    fig = fig.add_scattergl(
        x=x,
        y=y,
        uid=y_name,
        name=y_name,
        mode="markers",
        marker=dict(color="royalblue", symbol="circle-open", size=3),
        selected=dict(marker=dict(color="lightskyblue", opacity=0.5)),
        unselected=dict(marker=dict(opacity=0.1)),
        secondary_y=False,
    )
    fig = fig.update_xaxes(title_text=x_name)
    fig = fig.update_yaxes(title_text=y_name, secondary_y=False)
    if y2 is not None:
        fig = fig.add_scattergl(
            x=x,
            y=y2,
            uid=y2_name,
            name=y2_name,
            mode="markers",
            marker=dict(color="crimson", symbol="circle-open", size=3),
            selected=dict(marker=dict(opacity=0.5)),
            unselected=dict(marker=dict(opacity=0.1)),
            secondary_y=True,
        )
        fig = fig.update_yaxes(title_text=y2_name, secondary_y=True)
    fig = fig.update_layout(
        template="simple_white",
        dragmode="select",
        selectdirection="h",
        hovermode="x unified",
        hoverlabel=dict(namelength=-1),
        modebar=dict(activecolor="royalblue"),
    )
    return fig


def plot_fit(
    df: pl.DataFrame,
    result_info: 
)