import kfp
import kfp.dsl as dsl

from kfp import compiler
from kfp.dsl import Artifact, Input, Output

from typing import Dict, List

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-videointelligence']
)
def analyze_shots(video: str, output: Output[Artifact]):
    """Detects camera shot changes."""
    import json

    from google.cloud import videointelligence

    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [videointelligence.Feature.SHOT_CHANGE_DETECTION]
    operation = video_client.annotate_video(
        request={
            "features": features,
            "input_uri": video
        }
    )
    print("\nProcessing video for shot change annotations:")

    result = operation.result(timeout=7200)
    print("\nFinished processing.")

    # first result is retrieved because a single video was processed
    shots = []
    for i, shot in enumerate(result.annotation_results[0].shot_annotations):
        start_time = (
            shot.start_time_offset.seconds + shot.start_time_offset.microseconds / 1e6
        )
        end_time = (
            shot.end_time_offset.seconds + shot.end_time_offset.microseconds / 1e6
        )
        shots.append(
            {
                "start_time": start_time,
                "end_time": end_time,
            }
        )

    with open(output.path, 'w') as f:
        json.dump(shots, f)

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-videointelligence']
)
def analyze_text(video: str, output: Output[Artifact]):
    """ Detects camera shot changes. """
    import json

    from google.cloud import videointelligence

    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [videointelligence.Feature.TEXT_DETECTION]
    operation = video_client.annotate_video(
        request={
            "features": features,
            "input_uri": video
        }
    )
    print("\nProcessing video for text:")

    result = operation.result(timeout=7200)
    print("\nFinished processing.")

    # The first result is retrieved because a single video was processed.
    annotations = []
    for text_annotation in result.annotation_results[0].text_annotations:
        annotations.append({
            "text": text_annotation.text,
            "start_time": text_annotation.segments[0].segment.start_time_offset.seconds + text_annotation.segments[0].segment.start_time_offset.microseconds / 1e6,
            "end_time": text_annotation.segments[0].segment.end_time_offset.seconds + text_annotation.segments[0].segment.end_time_offset.microseconds / 1e6,
            "confidence": text_annotation.segments[0].confidence,
            "time_offset": text_annotation.segments[0].frames[0].time_offset.seconds + text_annotation.segments[0].frames[0].time_offset.microseconds / 1e6,
            "vertices": [
                {"x": vertex.x, "y": vertex.y} for vertex in text_annotation.segments[0].frames[0].rotated_bounding_box.vertices
            ]
        })

    with open(output.path, 'w') as f:
        json.dump(annotations, f)

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['av']
)
def extract_metadata(video: str, output: Output[Artifact]):
    import av
    import json

    container = av.open(video.replace("gs://", "/gcs/"))

    video_stream = next(s for s in container.streams if s.type == 'video')
    metadata = [{
        "width": video_stream.width,
        "height": video_stream.height,
        "duration": float(video_stream.duration * video_stream.time_base),
        "frame_rate": float(video_stream.average_rate)
    }]

    with open(output.path, 'w') as f:
        json.dump(metadata, f)


@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-bigquery', 'pandas', 'pyarrow']
)
def load_bigquery(dataset_id: str, table_id: str, video: str, input: Input[Artifact]):
    import json
    import pandas as pd
    from google.cloud import bigquery

    # Create a client instance
    client = bigquery.Client(project="CHANGE ME")

    # Load the JSON file into a pandas DataFrame
    data = pd.read_json(input.path, orient='records')
    data["video"] = video

    # Get the dataset reference
    dataset_ref = client.dataset(dataset_id)

    # Get the table reference
    table_ref = dataset_ref.table(table_id)

    # Load data into BigQuery if it's not empty
    if not data.empty:
        job = client.load_table_from_dataframe(data, table_ref)

        # Wait for the load job to complete
        job.result()

    print("Data loaded into BigQuery.")

@dsl.pipeline(
    name="video-intelligence"
)
def video_intelligence(videos: List[str]):
    with dsl.ParallelFor(
        name="videos",
        items=videos
    ) as video:
        analyze_shots_task = analyze_shots(
            video=video
        )
        load_bigquery(
            dataset_id="video_intelligence",
            table_id="shots",
            video=video,
            input=analyze_shots_task.output
        )
        analyze_text_task = analyze_text(
            video=video
        )
        load_bigquery(
            dataset_id="video_intelligence",
            table_id="text",
            video=video,
            input=analyze_text_task.output
        )
        extract_metadata_task = extract_metadata(
            video=video
        )
        load_bigquery(
            dataset_id="video_intelligence",
            table_id="metadata",
            video=video,
            input=extract_metadata_task.output
        )

compiler.Compiler().compile(video_intelligence, 'pipeline.json')