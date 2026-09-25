(function () {
    let currentRunId = null;
    let pollingTimer = null;
    let availableJobs = [];

    function showDataJobResult(message, type) {
        const box = document.getElementById("dataJobResult");
        if (!box) return;
        box.className = `alert alert-${type || "secondary"} mt-3 mb-0`;
        box.textContent = message;
    }

    function readInitializationStatus() {
        const node = document.getElementById("dataInitializationStatusPayload");
        if (!node || !node.textContent) {
            return null;
        }
        try {
            return JSON.parse(node.textContent);
        } catch (error) {
            console.error("解析初始化状态失败:", error);
            return null;
        }
    }

    function formatTime(text) {
        if (!text) return "-";
        try {
            return new Intl.DateTimeFormat("zh-CN", {
                timeZone: "Asia/Shanghai",
                year: "numeric",
                month: "2-digit",
                day: "2-digit",
                hour: "2-digit",
                minute: "2-digit",
                second: "2-digit",
                hour12: false,
            }).format(new Date(text));
        } catch (error) {
            return text;
        }
    }

    // 任务进度已并入最近任务表格：进度条内嵌在每行，点击行展开详情/日志
    let lastRuns = [];
    let latestRunDetail = null;

    function statusBadgeColor(status) {
        return {
            pending: "secondary",
            queued: "info",
            running: "primary",
            success: "success",
            failed: "danger",
            cancelled: "warning",
        }[status] || "secondary";
    }

    function progressBarHtml(progress, height) {
        const pct = Math.min(100, Math.max(0, typeof progress === "number" ? progress : 0));
        return `<div class="progress" style="height: ${height}px; min-width: 48px;">
            <div class="progress-bar" role="progressbar" style="width: ${pct}%;"></div>
        </div>`;
    }

    function renderDetailRow(run) {
        // 优先用轮询拿到的最新详情（含 result_json 日志）
        const d = latestRunDetail && latestRunDetail.id === run.id ? latestRunDetail : run;
        const status = d.status || "unknown";
        const resultJson = d.result_json || {};
        const lines = [];
        if (resultJson.stdout) lines.push(`[stdout]\n${resultJson.stdout}`);
        if (resultJson.stderr) lines.push(`[stderr]\n${resultJson.stderr}`);
        if (!resultJson.stdout && !resultJson.stderr && d.error_message) lines.push(d.error_message);
        const logHtml = lines.length
            ? `<pre class="small mt-2 mb-0" style="max-height: 180px; overflow-y: auto; white-space: pre-wrap;">${lines.join("\n\n")}</pre>`
            : "";
        return `<tr class="data-job-detail" style="cursor: pointer;" title="点击收起"><td colspan="8" class="bg-light">
            <div class="d-flex align-items-center gap-2 flex-wrap mb-1">
                <span class="badge bg-${statusBadgeColor(status)}">${status}</span>
                <span class="badge bg-light text-dark border">run #${d.id}</span>
                <span class="badge bg-light text-dark border">${d.job_type}</span>
                <span class="badge bg-light text-dark border">${(typeof d.progress === "number" ? d.progress : 0).toFixed(1)}%</span>
                <span class="text-muted">source=${d.source_name || "-"}/${d.source_mode || "-"}</span>
                <span class="text-muted">snapshot=${d.snapshot_tag || "-"}</span>
            </div>
            ${progressBarHtml(d.progress, 8)}
            ${d.error_message ? `<div class="text-danger small mt-1">${d.error_message}</div>` : ""}
            ${logHtml}
        </td></tr>`;
    }

    function renderRunHistory(runs) {
        const container = document.getElementById("dataJobHistory");
        if (!container) return;

        lastRuns = Array.isArray(runs) ? runs : [];

        if (lastRuns.length === 0) {
            container.textContent = "暂无任务历史";
            return;
        }

        const rows = lastRuns
            .map((run) => {
                const progress = typeof run.progress === "number" ? `${run.progress.toFixed(1)}%` : "-";
                const progressMessage = run.progress_message || "-";
                const sourceName = run.source_name || "-";
                const retryCell = ["success", "failed", "cancelled"].includes(run.status)
                    ? `<button type="button" class="btn btn-outline-secondary btn-sm" data-retry-run-id="${run.id}" title="按原参数重新执行；写入按交易日分区原子覆盖，不会产生重复数据">重试</button>`
                    : "";
                const deleteCell = ["success", "failed", "cancelled"].includes(run.status)
                    ? `<button type="button" class="btn btn-outline-danger btn-sm" data-delete-run-id="${run.id}">删除</button>`
                    : "";
                const detailRow = currentRunId === run.id ? renderDetailRow(run) : "";
                return `<tr data-run-id="${run.id}">
                    <td>${run.id}</td>
                    <td>${run.job_type}</td>
                    <td style="min-width: 110px;">
                        <div class="d-flex align-items-center gap-1">${progressBarHtml(run.progress, 6)}<span>${progress}</span></div>
                    </td>
                    <td>${sourceName}</td>
                    <td>${progressMessage}</td>
                    <td>${formatTime(run.finished_at || run.started_at || run.queued_at)}</td>
                    <td>${retryCell}${deleteCell}</td>
                    <td><span class="badge bg-${statusBadgeColor(run.status)}" title="${run.error_message || ""}">${run.status}</span></td>
                </tr>${detailRow}`;
            })
            .join("");

        container.innerHTML = `
            <div class="table-responsive">
                <table class="table table-sm table-hover mb-0">
                    <thead>
                        <tr>
                            <th>Run ID</th>
                            <th>任务</th>
                            <th>进度</th>
                            <th>来源</th>
                            <th>进度消息</th>
                            <th>时间</th>
                            <th>操作</th>
                            <th>状态</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;

        container.querySelectorAll("tbody tr[data-run-id]").forEach((row) => {
            row.addEventListener("click", function () {
                const runId = Number(this.getAttribute("data-run-id"));
                if (Number.isNaN(runId) || runId <= 0) return;
                if (currentRunId === runId) {
                    // 再次点击收起详情
                    currentRunId = null;
                    latestRunDetail = null;
                    stopPolling();
                    renderRunHistory(lastRuns);
                    return;
                }
                currentRunId = runId;
                latestRunDetail = null;
                renderRunHistory(lastRuns);
                fetchRunStatus(runId);
                startPolling(runId);
            });
        });

        // 展开的详情行点击同样收起（选中日志文本复制时不收起）
        container.querySelectorAll("tr.data-job-detail").forEach((row) => {
            row.addEventListener("click", function () {
                if (window.getSelection && window.getSelection().toString()) return;
                currentRunId = null;
                latestRunDetail = null;
                stopPolling();
                renderRunHistory(lastRuns);
            });
        });

        container.querySelectorAll("[data-retry-run-id]").forEach((button) => {
            button.addEventListener("click", function (event) {
                event.stopPropagation();
                const runId = Number(this.getAttribute("data-retry-run-id"));
                if (!Number.isNaN(runId) && runId > 0) {
                    retryRun(runId);
                }
            });
        });

        container.querySelectorAll("[data-delete-run-id]").forEach((button) => {
            button.addEventListener("click", function (event) {
                event.stopPropagation();
                const runId = Number(this.getAttribute("data-delete-run-id"));
                if (!Number.isNaN(runId) && runId > 0) {
                    deleteRun(runId);
                }
            });
        });
    }

    // 删除任务记录：仅终态任务可删（后端对活跃任务返回 400）
    async function deleteRun(runId) {
        if (!window.confirm(`确定删除任务 #${runId} 的记录吗？`)) return;
        try {
            const resp = await fetch(`/api/data-jobs/${runId}`, { method: "DELETE" });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                showDataJobResult(`删除失败: ${data.error || "未知错误"}`, "danger");
                return;
            }
            showDataJobResult(`已删除任务记录 run_id=${runId}`, "success");
            loadRunHistory();
        } catch (error) {
            console.error("删除任务失败:", error);
            showDataJobResult("删除失败，请检查服务状态", "danger");
        }
    }

    // 重试走 /retry API：后端按原 params_json 重新提交，不会丢日期参数
    async function retryRun(runId) {
        showDataJobResult(`正在重试 run_id=${runId} ...`, "info");
        try {
            const resp = await fetch(`/api/data-jobs/${runId}/retry`, { method: "POST" });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                showDataJobResult(`重试失败: ${data.error || "未知错误"}`, "danger");
                return;
            }
            showDataJobResult(`已重试：新 run_id=${data.run_id}, status=${data.status}`, "success");
            currentRunId = data.run_id;
            await fetchRunStatus(currentRunId);
            startPolling(currentRunId);
            loadRunHistory();
        } catch (error) {
            console.error("重试任务失败:", error);
            showDataJobResult("重试失败，请检查服务状态", "danger");
        }
    }

    async function loadRunHistory() {
        try {
            const resp = await fetch("/api/data-jobs/list?limit=10");
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                return;
            }
            renderRunHistory(data.runs || []);
        } catch (error) {
            console.error("加载任务历史失败:", error);
        }
    }

    async function fetchRunStatus(runId) {
        try {
            const resp = await fetch(`/api/data-jobs/${runId}`);
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                showDataJobResult(`获取任务状态失败: ${data.error || "未知错误"}`, "warning");
                return null;
            }
            latestRunDetail = data.run;
            // 轮询结果同步回列表行并重渲染：进度条/详情保持鲜活
            const idx = lastRuns.findIndex((r) => r.id === data.run.id);
            if (idx >= 0) {
                lastRuns[idx] = data.run;
                renderRunHistory(lastRuns);
            }
            return data.run;
        } catch (error) {
            console.error("获取任务状态失败:", error);
            return null;
        }
    }

    function stopPolling() {
        if (pollingTimer) {
            window.clearInterval(pollingTimer);
            pollingTimer = null;
        }
    }

    function startPolling(runId) {
        stopPolling();
        pollingTimer = window.setInterval(async function () {
            const run = await fetchRunStatus(runId);
            if (!run) return;
            if (["success", "failed", "cancelled"].includes(run.status)) {
                stopPolling();
                loadRunHistory();
            }
        }, 3000);
    }

    async function loadJobTypes() {
        const select = document.getElementById("jobTypeSelect");
        if (!select) return;

        try {
            const resp = await fetch("/api/data-jobs/jobs");
            const data = await resp.json();
            if (!data.success || !Array.isArray(data.jobs)) {
                return;
            }

            availableJobs = data.jobs.slice();
            select.innerHTML = "";
            data.jobs.forEach((job) => {
                const option = document.createElement("option");
                option.value = job.job_type;
                option.textContent = `${job.recommended_order || "-"} - ${job.display_name || job.job_type}`;
                select.appendChild(option);
            });
            updateSelectedJobMeta(select.value);
        } catch (error) {
            console.error("加载任务类型失败:", error);
        }
    }

    function updateSelectedJobMeta(jobType) {
        const job = availableJobs.find((item) => item.job_type === jobType);
        const displayName = document.getElementById("selectedJobDisplayName");
        const description = document.getElementById("selectedJobDescription");
        const group = document.getElementById("selectedJobGroup");
        const dependencies = document.getElementById("selectedJobDependencies");

        if (!displayName || !description || !group || !dependencies) return;

        if (!job) {
            displayName.textContent = "请选择任务";
            description.textContent = "任务描述会显示在这里。";
            group.textContent = "-";
            dependencies.textContent = "无";
            return;
        }

        displayName.textContent = job.display_name || job.job_type;
        description.textContent = job.description || "暂无任务说明。";
        group.textContent = job.group || "-";
        dependencies.textContent = Array.isArray(job.dependencies) && job.dependencies.length > 0
            ? job.dependencies.join(", ")
            : "无";
        const auditSummary = [job.source_name || "-", job.source_mode || "-"].join(" / ");
        description.textContent = `${job.description || "暂无任务说明。"} 数据来源: ${auditSummary}`;
    }

    function renderInitializationStatus(report) {
        const badge = document.getElementById("statusSummaryBadge");
        const connected = document.getElementById("statusDatabaseConnected");
        const missing = document.getElementById("statusMissingTables");
        const empty = document.getElementById("statusEmptyTables");
        const nextActions = document.getElementById("statusNextActions");

        if (!badge || !connected || !missing || !empty || !nextActions) {
            return;
        }

        if (!report || !report.database) {
            badge.className = "status-chip bg-light text-dark mb-3";
            badge.textContent = "状态不可用";
            connected.textContent = "-";
            missing.textContent = "-";
            empty.textContent = "-";
            nextActions.innerHTML = "<li>未获取到初始化状态。</li>";
            return;
        }

        const database = report.database;
        const ok = Boolean(database.ok);
        const connectedText = database.connected ? "正常" : "失败";
        const missingTables = Array.isArray(database.missing_tables) && database.missing_tables.length > 0
            ? database.missing_tables.join(", ")
            : "无";
        const emptyTables = Array.isArray(database.empty_tables) && database.empty_tables.length > 0
            ? database.empty_tables.join(", ")
            : "无";
        const actions = Array.isArray(database.next_actions) && database.next_actions.length > 0
            ? database.next_actions
            : ["暂无建议"];

        badge.className = ok
            ? "status-chip bg-success text-white mb-3"
            : "status-chip bg-warning text-dark mb-3";
        badge.textContent = ok ? "基础状态正常" : "需要初始化";
        connected.textContent = connectedText;
        missing.textContent = missingTables;
        empty.textContent = emptyTables;
        nextActions.innerHTML = actions.map((action) => `<li>${action}</li>`).join("");
    }

    function bindRecommendedJobButtons() {
        const buttons = document.querySelectorAll("[data-recommended-job]");
        const select = document.getElementById("jobTypeSelect");

        if (!select || buttons.length === 0) {
            return;
        }

        buttons.forEach((button) => {
            button.addEventListener("click", function () {
                const jobType = this.getAttribute("data-recommended-job");
                if (!jobType) {
                    return;
                }
                select.value = jobType;
                updateSelectedJobMeta(jobType);
            });
        });
    }

    async function submitDataJob() {
        const select = document.getElementById("jobTypeSelect");
        const startDate = document.getElementById("jobStartDate")?.value;
        const endDate = document.getElementById("jobEndDate")?.value;

        if (!select || !select.value) {
            showDataJobResult("请选择任务类型", "warning");
            return;
        }

        showDataJobResult("任务提交中...", "info");

        try {
            const payload = {
                job_type: select.value,
                params: {
                    start_date: startDate || "",
                    end_date: endDate || "",
                },
            };
            const resp = await fetch("/api/data-jobs/submit", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            const data = await resp.json();

            if (!resp.ok || !data.success) {
                showDataJobResult(`任务提交失败: ${data.error || "未知错误"}`, "danger");
                return;
            }

            showDataJobResult(
                `任务提交成功: run_id=${data.run_id}, status=${data.status}`,
                "success"
            );
            currentRunId = data.run_id;
            await fetchRunStatus(currentRunId);
            startPolling(currentRunId);
            loadRunHistory();
        } catch (error) {
            console.error("提交任务失败:", error);
            showDataJobResult("任务提交失败，请检查服务状态", "danger");
        }
    }

    document.addEventListener("DOMContentLoaded", function () {
        const submitButton = document.getElementById("submitDataJobBtn");
        const refreshButton = document.getElementById("refreshDataJobHistoryBtn");

        if (submitButton) {
            submitButton.addEventListener("click", submitDataJob);
        }
        if (refreshButton) {
            refreshButton.addEventListener("click", loadRunHistory);
        }
        const jobTypeSelect = document.getElementById("jobTypeSelect");
        if (jobTypeSelect) {
            jobTypeSelect.addEventListener("change", function () {
                updateSelectedJobMeta(this.value);
            });
        }

        bindRecommendedJobButtons();
        renderInitializationStatus(readInitializationStatus());
        loadJobTypes();
        loadRunHistory();

        // 大宽表按钮绑定
        var buildBtn = document.getElementById("buildWideTableBtn");
        var refreshWideBtn = document.getElementById("refreshWideTableStatusBtn");
        if (buildBtn) buildBtn.addEventListener("click", submitBuildWideTable);
        if (refreshWideBtn) refreshWideBtn.addEventListener("click", loadWideTableStatus);
        loadWideTableStatus();
    });

    // ---- 大宽表状态与构建 ----

    async function loadWideTableStatus() {
        var badge = document.getElementById("wideTableStatusBadge");
        var dateEl = document.getElementById("wideTableDate");
        var sourceDatesEl = document.getElementById("wideTableSourceDates");
        var reasonEl = document.getElementById("wideTableUpdateReason");
        var buildBtn = document.getElementById("buildWideTableBtn");
        if (!badge) return;

        badge.className = "badge bg-secondary";
        badge.textContent = "检查中...";

        try {
            var resp = await fetch("/api/data-jobs/wide-table/status");
            var data = await resp.json();
            if (!data.success) {
                badge.className = "badge bg-danger";
                badge.textContent = "查询失败";
                return;
            }

            var s = data.status;
            dateEl.textContent = s.wide_table_date || "文件不存在";

            var parts = [];
            if (s.source_dates) {
                for (var table in s.source_dates) {
                    parts.push(table + ": " + (s.source_dates[table] || "无"));
                }
            }
            sourceDatesEl.textContent = parts.join(" | ");

            if (!s.exists) {
                badge.className = "badge bg-danger";
                badge.textContent = "缺失";
                buildBtn.disabled = !s.past_cutoff;
                reasonEl.textContent = "宽表文件不存在" + (s.past_cutoff ? "，可以构建" : "，需等待 18:00 后");
            } else if (s.should_update) {
                badge.className = "badge bg-warning text-dark";
                badge.textContent = "需更新";
                buildBtn.disabled = !s.past_cutoff;
                reasonEl.textContent = s.reason + (s.past_cutoff ? "" : "（需等待 18:00 后）");
            } else {
                badge.className = "badge bg-success";
                badge.textContent = "正常";
                buildBtn.disabled = true;
                reasonEl.textContent = "宽表已是最新";
            }
        } catch (err) {
            badge.className = "badge bg-danger";
            badge.textContent = "网络错误";
        }
    }

    async function submitBuildWideTable() {
        var buildBtn = document.getElementById("buildWideTableBtn");
        var resultBox = document.getElementById("wideTableBuildResult");

        buildBtn.disabled = true;
        resultBox.style.display = "block";
        resultBox.className = "alert alert-info mt-3";
        resultBox.textContent = "正在提交大宽表构建任务...";

        try {
            var resp = await fetch("/api/data-jobs/wide-table/build", { method: "POST" });
            var data = await resp.json();

            if (data.success) {
                resultBox.className = "alert alert-success mt-3";
                resultBox.textContent = "构建任务已提交 (run_id=" + data.run_id + ")，请查看下方日频数据中心的进度。";
                currentRunId = data.run_id;
                await fetchRunStatus(currentRunId);
                startPolling(currentRunId);
                loadRunHistory();
            } else {
                resultBox.className = "alert alert-danger mt-3";
                resultBox.textContent = "构建失败: " + data.error;
                buildBtn.disabled = false;
            }
        } catch (err) {
            resultBox.className = "alert alert-danger mt-3";
            resultBox.textContent = "网络错误: " + err.message;
            buildBtn.disabled = false;
        }

        setTimeout(loadWideTableStatus, 5000);
    }
})();
