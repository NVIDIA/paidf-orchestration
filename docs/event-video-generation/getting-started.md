# Event Video Generation Getting Started

Complete the shared [Getting Started Guide](../getting-started.md) through
opening the Airflow UI, then return here to run Event Video Generation.

## Run the Workflow

1. **Construct and Upload Input Data**:

   Upload one source image or a directory of source images to S3. Event Video
   Generation supports `.jpg`, `.jpeg`, `.png`, `.bmp`, `.gif`, `.tiff`, and
   `.webp` files. For example:

   ```text
   event-video-input/
      warehouse-entrance.jpg
      loading-dock.png
   ```

2. **Create a Starter Payload**:

   For in-cluster services, start with
   [`warehouse-safety-mix-internal-10.json`](../../payload/event_video_generation_dag/warehouse-safety-mix-internal-10.json).
   For external endpoints, start with
   [`warehouse-safety-mix-external-10.json`](../../payload/event_video_generation_dag/warehouse-safety-mix-external-10.json).
   In either case, set:

   - `input_path` to your image or image-directory S3 path.
   - `output_directory` to your desired output path, for example
     `s3://<your-output-bucket>/<your-workflow>/output`.

   For external services, also replace the VLM, LLM, and image-to-video endpoint
   placeholders in `cosmos`. See the [Event Video Generation Payload Guide](payload-guide.md)
   for all fields and additional starter payloads.

3. **Start the Workflow from the UI**:

   > Only run one workflow at a time to avoid long queueing times or unexpected
   > issues.

   1. From the Airflow home page, select **Dags** in the left sidebar.
   2. Select `event_video_generation_dag_k8s`.
   3. Click the trigger button in the upper-right corner.
   4. Under **Run Parameters**, update `EventVideoGenerationDagPayloadConfig`
      with the payload from the previous step.
   5. Select **Trigger** to start the run.

4. **Monitor or Stop an Active Run**:

   Monitor progress in Airflow. To cancel a run, open the active DagRun in
   **Grid** or **Graph** view, find the running task, and mark it as failed.
   This lets the DAG run service shutdown and clean up its resources.

   **Do not delete the DagRun or DAG** from the UI. Deleting a run bypasses the
   shutdown path and can leave stale Deployments, Services, or GPU pods.

5. **Inspect the Workflow Output**:

   The final anomaly dataset is written to
   `<output_directory>/<run_id>/anomaly_dataset/`. Its `dataset.json` manifest
   lists every generated scene and its video and annotation paths.
