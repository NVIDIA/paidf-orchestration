// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Renders the orchestration statistics dashboard from the report JSON embedded
// alongside it. This is a view: every metric it shows is read from the report
// rather than recomputed, and task categories are resolved server-side, so the
// dashboard has no knowledge of the DAG's task IDs.

(function () {
  "use strict";

  function readEmbeddedJson(elementId) {
    return JSON.parse(document.getElementById(elementId).textContent);
  }

  const report = readEmbeddedJson("report-data");
  const taskCategories = readEmbeddedJson("task-categories");
  const display = readEmbeddedJson("dashboard-config");

  const tasks = report.task_instances || [];
  const workload = report.workload || {};
  const throughput = report.throughput || {};
  const elapsed = report.dag_run.elapsed_seconds || 0;
  const startMs = Date.parse(report.dag_run.started_at);

  const HTML_ENTITIES = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  };

  function esc(value) {
    return String(value == null ? "" : value).replace(/[&<>"']/g, function (character) {
      return HTML_ENTITIES[character];
    });
  }

  function duration(seconds) {
    if (seconds == null) {
      return "—";
    }
    if (seconds < 60) {
      return seconds.toFixed(seconds < 10 ? 2 : 1) + "s";
    }
    return Math.floor(seconds / 60) + "m " + (seconds % 60).toFixed(1) + "s";
  }

  function shortTask(taskId) {
    return taskId
      .split(".")
      .map(function (part) {
        return part.replaceAll("_", " ");
      })
      .join(" › ");
  }

  function mapIndexSuffix(task) {
    return task.map_index >= 0 ? " [" + task.map_index + "]" : "";
  }

  function category(taskId) {
    return taskCategories[taskId] || "orchestration";
  }

  function average(values) {
    const present = values.filter(function (value) {
      return value != null;
    });
    if (!present.length) {
      return 0;
    }
    return (
      present.reduce(function (total, value) {
        return total + value;
      }, 0) / present.length
    );
  }

  function itemRate(itemsPerMinute) {
    return itemsPerMinute == null ? "—" : Number(itemsPerMinute).toFixed(3) + " " + display.unit_plural + "/min";
  }

  function countLabel(count) {
    return count === 1 ? "1 " + display.unit_singular : count + " " + display.unit_plural;
  }

  function setText(elementId, text) {
    document.getElementById(elementId).textContent = text;
  }

  function setHtml(elementId, html) {
    document.getElementById(elementId).innerHTML = html;
  }

  // --- Header -------------------------------------------------------------

  setText("dag", report.dag_run.dag_id);
  setText("report-title", display.title);
  setText("run-id", report.dag_run.run_id);
  setText(
    "started",
    report.dag_run.started_at == null ? "—" : new Date(report.dag_run.started_at).toLocaleString()
  );

  // --- Summary cards ------------------------------------------------------

  const finalCount = workload[display.final_count_key] || 0;
  const successful = workload[display.successful_count_key] || 0;
  const stageSeconds = throughput.augmentation_stage_wall_seconds;
  const endToEndRate = (throughput[display.end_to_end_rate_key] || {}).items_per_minute;
  const augmentationRate = (throughput[display.augmentation_rate_key] || {}).items_per_minute;
  const queueValues = tasks
    .map(function (task) {
      return task.queued_duration_seconds;
    })
    .filter(function (value) {
      return value != null;
    });
  const maxQueue = queueValues.length ? Math.max.apply(null, queueValues) : 0;
  const observed = finalCount
    ? countLabel(finalCount) + " / " + duration(elapsed)
    : "No final " + display.unit_plural;
  const stageObserved = successful
    ? countLabel(successful) + " / " + duration(stageSeconds)
    : "No " + display.successful_label;
  const measurementDetail = report.dag_run.measurement_end_task_id
    ? "DAG start → validated output"
    : "DAG start → report collection (fallback)";
  const stageShare = (elapsed ? (stageSeconds / elapsed) * 100 : 0).toFixed(1);

  function card(label, value, detail) {
    return (
      '<article class="card">' +
      '<div class="label">' +
      esc(label) +
      "</div>" +
      '<div class="value">' +
      esc(value) +
      "</div>" +
      '<div class="detail">' +
      esc(detail) +
      "</div>" +
      "</article>"
    );
  }

  setHtml(
    "cards",
    [
      card("Measured window", duration(elapsed), measurementDetail),
      card(
        display.final_label,
        String(finalCount),
        workload[display.input_count_key] +
          " " + display.input_label + " · " +
          workload[display.per_input_count_key] +
          " " + display.per_input_label,
      ),
      card("End-to-end throughput", itemRate(endToEndRate), observed),
      card(display.stage_label + " throughput", itemRate(augmentationRate), stageObserved),
      card(display.stage_label + " stage", duration(stageSeconds), stageShare + "% of measured window"),
      card("Longest queue", duration(maxQueue), "Average " + duration(average(queueValues))),
    ].join(""),
  );

  if (finalCount === 0) {
    setText("sample-text", "No final " + display.unit_plural + " were produced, so throughput is unavailable.");
  } else if (finalCount === 1) {
    setText(
      "sample-text",
      "One final " + display.unit_singular + " took " +
        duration(elapsed) +
        "; " +
        itemRate(endToEndRate) +
        " is the reciprocal of that latency, not a steady-state capacity estimate.",
    );
  } else {
    setText("sample-text", "Throughput is " + itemRate(endToEndRate) + " (" + observed + ").");
  }

  // --- Execution timeline -------------------------------------------------

  const axis = document.getElementById("axis");
  for (let i = 0; i <= 5; i++) {
    const tick = document.createElement("span");
    tick.className = "tick";
    tick.style.left = i * 20 + "%";
    tick.textContent = duration((elapsed * i) / 5);
    axis.appendChild(tick);
  }

  const timelineTasks = tasks
    .filter(function (task) {
      return task.started_at && task.ended_at && task.task_duration_seconds > 0;
    })
    .sort(function (a, b) {
      return Date.parse(a.started_at) - Date.parse(b.started_at);
    });

  setHtml(
    "timeline",
    timelineTasks
      .map(function (task) {
        const offset = Math.max(0, (Date.parse(task.started_at) - startMs) / 1000);
        const left = Math.min(100, (offset / elapsed) * 100);
        const width = Math.max(
          0.55,
          Math.min(100 - left, (task.task_duration_seconds / elapsed) * 100),
        );
        const barTitle =
          shortTask(task.task_id) +
          " · start +" +
          duration(offset) +
          " · runtime " +
          duration(task.task_duration_seconds);
        return (
          '<div class="timeline-row">' +
          '<div class="task-label" title="' +
          esc(task.task_id) +
          '">' +
          esc(shortTask(task.task_id)) +
          mapIndexSuffix(task) +
          "</div>" +
          '<div class="track"><span class="bar ' +
          category(task.task_id) +
          '" style="left:' +
          left +
          "%;width:" +
          width +
          '%" title="' +
          esc(barTitle) +
          '"></span></div>' +
          "</div>"
        );
      })
      .join(""),
  );

  // --- Queue time vs runtime ----------------------------------------------

  const measured = tasks
    .filter(function (task) {
      return task.task_duration_seconds > 0 && task.queued_duration_seconds != null;
    })
    .sort(function (a, b) {
      return b.task_duration_seconds - a.task_duration_seconds;
    });
  const scale = measured.length
    ? Math.max.apply(
        null,
        measured.map(function (task) {
          return Math.max(task.task_duration_seconds, task.queued_duration_seconds);
        }),
      )
    : 1;

  setHtml(
    "metrics",
    measured
      .map(function (task) {
        return (
          '<div class="metric-row">' +
          '<div class="metric-label" title="' +
          esc(task.task_id) +
          '">' +
          esc(shortTask(task.task_id)) +
          "</div>" +
          '<div class="dual">' +
          '<span class="queue" style="width:' +
          Math.max(1, (task.queued_duration_seconds / scale) * 100) +
          '%"></span>' +
          '<span class="run" style="width:' +
          Math.max(1, (task.task_duration_seconds / scale) * 100) +
          '%"></span>' +
          "</div>" +
          '<div class="metric-value">' +
          duration(task.queued_duration_seconds) +
          " / " +
          duration(task.task_duration_seconds) +
          "</div>" +
          "</div>"
        );
      })
      .join(""),
  );

  // --- Operational signals ------------------------------------------------

  function insight(value, label) {
    return (
      '<div class="insight"><strong>' + value + "</strong><span>" + esc(label) + "</span></div>"
    );
  }

  const slowest = measured[0];
  const successCount = tasks.filter(function (task) {
    return task.state === "success";
  }).length;

  setHtml(
    "insights",
    [
      insight(duration(average(queueValues)), "Average task queue delay"),
      slowest
        ? insight(
            esc(shortTask(slowest.task_id)),
            "Longest task at " + duration(slowest.task_duration_seconds),
          )
        : "",
      insight(
        successful + " / " + workload.expected_augmentations,
        "Successful / expected augmented " + display.unit_plural,
      ),
      insight(successCount + " / " + tasks.length, "Tasks successful at report collection"),
    ].join(""),
  );

  // --- Task detail table --------------------------------------------------

  const ordered = tasks.slice().sort(function (a, b) {
    const aStart = a.started_at ? Date.parse(a.started_at) : Infinity;
    const bStart = b.started_at ? Date.parse(b.started_at) : Infinity;
    return aStart - bStart || a.task_id.localeCompare(b.task_id);
  });

  setHtml(
    "task-table",
    ordered
      .map(function (task) {
        const offset = task.started_at
          ? Math.max(0, (Date.parse(task.started_at) - startMs) / 1000)
          : null;
        const state = task.state || "unknown";
        return (
          "<tr>" +
          '<td title="' +
          esc(task.task_id) +
          '">' +
          esc(shortTask(task.task_id)) +
          mapIndexSuffix(task) +
          "</td>" +
          '<td><span class="state ' +
          esc(state) +
          '">' +
          esc(state) +
          "</span></td>" +
          "<td>" +
          esc(task.pool) +
          "</td>" +
          '<td class="num">' +
          duration(task.queued_duration_seconds) +
          "</td>" +
          '<td class="num">' +
          duration(task.task_duration_seconds) +
          "</td>" +
          '<td class="num">' +
          duration(offset) +
          "</td>" +
          "</tr>"
        );
      })
      .join(""),
  );

  setText(
    "footer",
    "Generated from " +
      report.airflow_api.task_instance_count +
      " Airflow task instances · report collected " +
      new Date(report.generated_at).toLocaleString() +
      " · YAML: " +
      report.artifacts.yaml,
  );
})();
