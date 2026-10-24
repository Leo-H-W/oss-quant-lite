/**
 * 股票代码/名称联想下拉。
 * 依赖 base.html 中的 apiRequest；后端接口 /api/stocks/suggest。
 *
 * 用法：
 *   attachStockSuggest(inputEl, {
 *     onSelect(item) { ... },   // item = { ts_code, symbol, name }
 *     getQuery(value) { ... },  // 从输入框当前值提取检索词，默认取点号前部分
 *     limit: 10,
 *   });
 */
(function (global) {
  let seq = 0;

  function attachStockSuggest(input, options = {}) {
    if (!input) return;
    const limit = options.limit || 10;
    const onSelect = options.onSelect || ((item) => { input.value = item.ts_code; });
    const getQuery = options.getQuery || ((value) => value.split('.')[0].trim());

    const wrapper = input.parentElement;
    if (wrapper) wrapper.classList.add('position-relative');
    input.setAttribute('autocomplete', 'off');

    const dropdown = document.createElement('div');
    dropdown.className = 'list-group position-absolute w-100 shadow stock-suggest-dropdown';
    dropdown.style.cssText = 'z-index:1050;max-height:320px;overflow-y:auto;display:none;';
    input.insertAdjacentElement('afterend', dropdown);

    let timer = null;

    function hide() {
      dropdown.style.display = 'none';
      dropdown.innerHTML = '';
    }

    async function fetchSuggest(q) {
      const mySeq = ++seq;
      try {
        const resp = await apiRequest(`/stocks/suggest?q=${encodeURIComponent(q)}&limit=${limit}`);
        if (mySeq !== seq) return;  // 过期响应丢弃
        if (resp.code === 200) render(resp.data);
      } catch (e) {
        console.error('搜索联想失败:', e);
        hide();
      }
    }

    function render(items) {
      if (!items || items.length === 0) { hide(); return; }
      dropdown.innerHTML = items.map(item => `
        <button type="button" class="list-group-item list-group-item-action"
                data-ts-code="${item.ts_code}" data-symbol="${item.symbol}" data-name="${item.name}">
          <code>${item.ts_code}</code> (${item.name})
        </button>`).join('');
      dropdown.style.display = 'block';
      dropdown.querySelectorAll('button').forEach(el => {
        el.addEventListener('click', function () {
          onSelect({
            ts_code: this.dataset.tsCode,
            symbol: this.dataset.symbol,
            name: this.dataset.name,
          });
          hide();
        });
      });
    }

    input.addEventListener('input', function () {
      clearTimeout(timer);
      const q = getQuery(input.value);
      if (!q) { hide(); return; }
      timer = setTimeout(() => fetchSuggest(q), 200);
    });
    input.addEventListener('keydown', (e) => { if (e.key === 'Escape') hide(); });
    // blur 延迟收起，让下拉项的 click 先触发
    input.addEventListener('blur', () => setTimeout(hide, 150));
    document.addEventListener('click', (e) => {
      if (e.target !== input && !dropdown.contains(e.target)) hide();
    });
  }

  global.attachStockSuggest = attachStockSuggest;
})(window);
