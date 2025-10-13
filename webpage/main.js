class token_row {
    constructor(element) {
        this.element = element; // is of type Element
        this.element.classList.add('token-row');
        this.element.addEventListener('click', this.on_click.bind(this));
        this.attributes = {}; 
        this.asset_id = null
    }
    
    on_click() {
        if (this.asset_id != null) {
            // todo
        }
    }
};


const tokens_per_page = 15;
const token_rows = new Array(tokens_per_page);
const refresh_sleep = 3;
let set_page_overview_text = null
const state = {
  attributes: 'm.market_id, m.condition_id, m.negrisk_id, b.server_time, a.bid_depth_diff, a.ask_depth_diff',
  filter: '',
  order: '',
  ascending: false,
  grouping: '',
  offset: 0,
  max_offset: 0,
  last_refresh: -refresh_sleep
};

function sleep(ms) {
    return new Promise(r => setTimeout(r, ms));
}

function render_rows(response_json) {
    try {
        state.max_offset = response_json["all_rows_count"] - 1;
        update_page_overview();

        const page_rows = response_json["page_rows"] || [];
        for (let row = 0; row < tokens_per_page; row++) {
            const row_data = page_rows[row] || {};
            let new_content = `offset: ${state.offset + row}      `;

            for (const [attribute, value] of Object.entries(row_data)) {
                new_content += `${attribute}: ${value}      `;
            }

            token_rows[row].element.textContent = new_content;
        }
    } catch (err) {
        console.error("render_rows failed:", err);
    }
}


async function refresh_row() {
    state.last_refresh = performance.now();
    try{
        const response = await fetch("http://127.0.0.1:8080/refresh", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({attributes: state.attributes, filter: state.filter, order: state.order, ascending: state.ascending, grouping: state.grouping, offset: state.offset, limit: tokens_per_page})
        });
        const data = await response.json();
        console.log(data);
        render_rows(data);
    } catch (err) {
        console.error("refresh_rows failed to fetch with error: ", err);
    }
}

async function refresh_rows() {
    console.log("refresh_rows starting")
    while (true) {
        const now = performance.now();
        const diff = refresh_sleep * 1000 - (now - state.last_refresh);
        if (diff > 0) await sleep(diff);
        await refresh_row();
    }
}

const ATTRIBUTE_LOOKUP = {
    string: new Set([
        "m.market_id",
        "m.condition_id",
        "m.negrisk_id",
        "m.asset_id",
        "b.asset_id",
        "c.asset_id",
        "tc.asset_id",
        "a.asset_id"

    ]),
    numeric: new Set([
        "m.row_index", "m.insert_time",
        "b.row_index", "b.resub_count", "b.insert_time", "b.server_time",
        "c.row_index", "c.resub_count", "c.insert_time", "c.server_time", "c.type",
        "tc.row_index", "tc.resub_count", "tc.insert_time", "tc.server_time",
        "a.row_index", "a.resub_count", "a.insert_time", "a.server_time", "a.ws_books_count",
        "a.ws_ambiguous_start_count", "a.ws_ambiguous_end_count", "a.bid_depth_diff",
        "a.ask_depth_diff", "a.bids_distance", "a.asks_distance",
        "a.ws_tick_changes_count", "a.ws_tick_changes", "a.tick_size_distance"
    ])
};


function render_grouping_panel(panel) {
    function validate_grouping_input(input) {
        if (!input || input.trim() === "") return true;

        const terms = input.split(",").map(t => t.trim()).filter(t => t.length > 0);
        for (const term of terms) {
            if (!ATTRIBUTE_LOOKUP.string.has(term)) {
                console.warn("Invalid grouping attribute:", term);
                return false;
            }
        }
        return true;
    }

    const docs = document.createElement('div');
    docs.textContent = `In the input box below you can specify the grouping e.g 'm.negrisk_id' which by default is ''\n
    The syntax is given by following NBNF:\n
        <grouping> ::= <string_attr> )*\n
        <string_attr> ::= "m.market_id" | "m.condition_id" | "m.negrisk_id"\n
    
    \n
    If a syntax error is detected the change can not be applied.
    `;

    const input_panel = document.createElement('div');
    input_panel.classList.add('grouping-panel');
    let is_valid_input = true;

    const input_el = document.createElement('input');
    input_el.value = state.grouping;
    input_el.classList.add('grouping-input');

    const apply_btn = document.createElement('button');
    apply_btn.textContent = "Apply";
    apply_btn.classList.add('grouping-apply-btn');

    input_el.addEventListener('input', () => {
        is_valid_input = validate_grouping_input(input_el.value);
        apply_btn.disabled = !is_valid_input;
    });

    apply_btn.addEventListener('click', () => {
        if (!is_valid_input) return;
        state.grouping = input_el.value.trim();
        console.log("Updated grouping:", state.grouping);
        apply_btn.textContent = "Applied";
        setTimeout(() => apply_btn.textContent = "Apply", 1000);
    });

    input_panel.append(input_el, apply_btn);
    panel.append(docs, input_panel);
}

function render_order_panel(panel) {
    function validate_order_input(input) {
        if (!input || input.trim() === "") return true;

        const tokens = input.split(/[\+\-]/).map(t => t.trim()).filter(t => t.length > 0);

        for (const token of tokens) {
            if (/^[0-9]+(\.[0-9]+)?$/.test(token)) continue;

            if (!ATTRIBUTE_LOOKUP.numeric.has(token)) {
                console.warn("Invalid numeric term in order:", token);
                return false;
            }
        }

        return true;
    }

    const docs = document.createElement('div');
    docs.textContent = `In the input box below you can specify the order which by default is 'm.insert_time'.\n
     The syntax is given by following NBNF:\n
        <order> ::= <numeric_term> ( ("+" | "-") <numeric_term> )*\n
        <numeric_term> ::= <numeric_attr> | <number>\n
        <numeric_attr> ::= "m.row_index" | "m.insert_time" |\n
        "b.row_index" | "b.resub_count" | "b.insert_time" | "b.server_time" |\n
        "c.row_index" | "c.resub_count" | "c.insert_time" | "c.server_time" | "c.type" |\n
        "tc.row_index" | "tc.resub_count" | "tc.insert_time" | "tc.server_time" |\n
        "a.row_index" | "a.resub_count" | "a.insert_time" | "a.server_time" |\n
        "a.ws_books_count" | "a.ws_ambiguous_start_count" | "a.ws_ambiguous_end_count" |\n
        "a.bid_depth_diff" | "a.ask_depth_diff" | "a.bids_distance" | "a.asks_distance" |\n
        "a.ws_tick_changes_count" | "a.ws_tick_changes" | "a.tick_size_distance"\n
        <number> ::= [0-9]+ ("." [0-9]+)?\n
        \n
    \n
    If a syntax error is detected the change can not be applied.
    `;

    const input_panel = document.createElement('div');
    input_panel.classList.add('order-panel');
    let is_valid_input = true;

    const input_el = document.createElement('input');
    input_el.value = state.order;
    input_el.classList.add('order-input');

    const apply_btn = document.createElement('button');
    apply_btn.textContent = "Apply";
    apply_btn.classList.add('order-apply-btn');

    input_el.addEventListener('input', () => {
        is_valid_input = validate_order_input(input_el.value);
        apply_btn.disabled = !is_valid_input;
    });

    apply_btn.addEventListener('click', () => {
        if (!is_valid_input) return;
        state.order = input_el.value.trim();
        console.log("Updated order:", state.order);
        apply_btn.textContent = "Applied";
        setTimeout(() => apply_btn.textContent = "Apply", 1000);
    });

    const ascending_btn = document.createElement('button');
    ascending_btn.textContent = "Ascending";
    ascending_btn.classList.add('ascending-btn');
    ascending_btn.addEventListener('click', () => {
        if (!state.ascending) {
            state.ascending = true;
            console.log("Order set to ascending");
        }
    });

    const descending_btn = document.createElement('button');
    descending_btn.textContent = "Descending";
    descending_btn.classList.add('descending-btn');
    descending_btn.addEventListener('click', () => {
        if (state.ascending) {
            state.ascending = false;
            console.log("Order set to descending");
        }
    });

    input_panel.append(input_el, apply_btn, ascending_btn, descending_btn);
    panel.append(docs, input_panel);
}

function render_filter_panel(panel) {
    function validate_filter_input(input) {
        if (!input || input.trim() === "") return true;

        const tokens = input.replace(/\(/g, ' ( ').replace(/\)/g, ' ) ').trim().split(/\s+/);
        const stack = [];
        let expectOperand = true;

        for (let i = 0; i < tokens.length; i++) {
            let token = tokens[i];

            if (token === '(') {
                stack.push('(');
                expectOperand = true;
            } else if (token === ')') {
                if (stack.length === 0) return false;
                stack.pop();
                expectOperand = false;
            } else if (/^(AND|OR)$/i.test(token)) {
                if (expectOperand) return false;
                expectOperand = true;
            } else if (token === 'NOT') {
                if (!expectOperand) return false;
            } else {
                let condTokens = [token];
                while (i + 1 < tokens.length && !['AND','OR','NOT','(',')'].includes(tokens[i+1])) {
                    i++;
                    condTokens.push(tokens[i]);
                }
                const condStr = condTokens.join(' ');

                const string_match = condStr.match(/^([a-z]+\.[a-z_]+)\s*(=|!=)\s*'[^']*'$/i);
                const null_match = condStr.match(/^([a-z]+\.[a-z_]+)\s+IS\s+NULL$/i);
                const num_match = condStr.match(/^(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)$/);

                if (string_match) {
                    if (!ATTRIBUTE_LOOKUP.string.has(string_match[1])) return false;
                } else if (null_match) {
                    if (!ATTRIBUTE_LOOKUP.string.has(null_match[1])) return false;
                } else if (num_match) {
                    const parts = [...num_match[1].split(/[\+\-]/), ...num_match[3].split(/[\+\-]/)].map(t => t.trim());
                    if (!parts.every(p => /^[0-9]+(\.[0-9]+)?$/.test(p) || ATTRIBUTE_LOOKUP.numeric.has(p))) return false;
                } else {
                    return false;
                }

                expectOperand = false;
            }
        }

        return stack.length === 0 && !expectOperand;
    }

    const docs = document.createElement('pre');
    docs.textContent = `In the input box below you can specify the filter which by default is '' (no filter).\n
    The syntax is given by following NBNF:\n
        <filter> ::= <clause> ( ( " AND " | " OR " ) [ "NOT " ] <clause> )*
        <clause> ::= [ "NOT " ] ( <condition> | "(" <filter> ")" )
        <condition> ::= <string_condition> | <numeric_condition><\n
        <string_condition> ::= <string_attr> ( <string_operator> ( <string_const> | <string_attr> ) | "IS NULL" )\n
        <string_operator> ::= "=" | "!="
        <string_attr> ::= "m.market_id" | "m.condition_id" | "m.negrisk_id"\n
        <string_const> ::= any string like: 'this', with explicit ' around it\n
        <numeric_condition> ::= <numeric_expr> <numeric_operator> <numeric_expr>\n
        <numeric_operator> ::= "=" | "!=" | ">" | "<" | ">=" | "<="\n
        <numeric_expr> ::= <numeric_term> ( ("+" | "-") <numeric_term> )*\n
        <numeric_term> ::= <numeric_attr> | <number>\n
        <numeric_attr> ::= "m.row_index" | "m.insert_time" |\n
        "b.row_index" | "b.resub_count" | "b.insert_time" | "b.server_time" |\n
        "c.row_index" | "c.resub_count" | "c.insert_time" | "c.server_time" | "c.type" |\n
        "tc.row_index" | "tc.resub_count" | "tc.insert_time" | "tc.server_time" |\n
        "a.row_index" | "a.resub_count" | "a.insert_time" | "a.server_time" |\n
        "a.ws_books_count" | "a.ws_ambiguous_start_count" | "a.ws_ambiguous_end_count" |\n
        "a.bid_depth_diff" | "a.ask_depth_diff" | "a.bids_distance" | "a.asks_distance" |\n
        "a.ws_tick_changes_count" | "a.ws_tick_changes" | "a.tick_size_distance"\n
        <number> ::= [0-9]+ ("." [0-9]+)?\n
        \n
    For example: 'm.market_id = 'huzz' AND (NOT m.negrisk_id IS NULL OR a.bids_distance + 5 > 0.01)'\n
    \n
    If a syntax error is detected the change can not be applied.
    `
    const input_panel = document.createElement('div');
    input_panel.classList.add('filter-panel');
    let is_valid_input = true;
    const apply_btn = document.createElement('button');
    apply_btn.textContent = "Apply";
    apply_btn.disabled = false;
    apply_btn.classList.add('filter-apply-btn');

    const input_el = document.createElement('input');
    input_el.value = state.filter || '';
    input_el.classList.add('filter-input');

    input_el.addEventListener('input', () => {
        is_valid_input = validate_filter_input(input_el.value);
        apply_btn.disabled = !is_valid_input;
        input_el.style.borderColor = is_valid_input ? 'green' : 'red';
    });

    apply_btn.addEventListener('click', () => {
        if (!is_valid_input) return;
        state.filter = input_el.value.trim();
        console.log("Updated filter:", state.filter);
        apply_btn.textContent = "Applied";
        setTimeout(() => apply_btn.textContent = "Apply", 1000);
    });

    input_panel.append(input_el, apply_btn);
    panel.append(docs, input_panel);
}

function render_attributes_panel(panel) {
    function validate_attributes_input(input) {
        if (!input || input.trim() === "") return true;

        const terms = input.split(",").map(s => s.trim()).filter(s => s.length > 0);

        for (const term of terms) {
            const [expr_part] = term.split(/\s+AS\s+/i);
            const expr = expr_part.trim();

            const tokens = expr.split(/[\+\-]/).map(t => t.trim()).filter(t => t.length > 0);

            for (const token of tokens) {
                if (/^[0-9]+(\.[0-9]+)?$/.test(token)) continue;
                if (/^'.*'$/.test(token)) continue;
                if (!ATTRIBUTE_LOOKUP.string.has(token) && !ATTRIBUTE_LOOKUP.numeric.has(token)) {
                    console.warn("Invalid attribute token:", token);
                    return false;
                }
                if ((ATTRIBUTE_LOOKUP.string.has(token) || /^'.*'$/.test(token)) && tokens.length > 1) {
                    console.warn("String used in numeric expression:", token);
                    return false;
                }
            }
        }

        return true;
    }

    const docs = document.createElement('pre');
    docs.textContent = `In the input box below you can specify the attributes which will show up in the token rows.\n
    The syntax is given by following NBNF:\n
        <attributes> ::= (<term> [" AS " <string_const>])*\n
        <term> ::= <string_term> | <numeric_expr><\n
        <string_term> ::= <string_attr> | <string_const>\n
        <string_attr> ::= "m.market_id" | "m.condition_id" | "m.negrisk_id"\n
        <string_const> ::= any string like: 'this', with explicit ' around it\n
        <numeric_expr> ::= <numeric_term> ( ("+" | "-") <numeric_term> )*\n
        <numeric_term> ::= <numeric_attr> | <number>\n
        <numeric_attr> ::= "m.row_index" | "m.insert_time" |\n
        "b.row_index" | "b.resub_count" | "b.insert_time" | "b.server_time" |\n
        "c.row_index" | "c.resub_count" | "c.insert_time" | "c.server_time" | "c.type" |\n
        "tc.row_index" | "tc.resub_count" | "tc.insert_time" | "tc.server_time" |\n
        "a.row_index" | "a.resub_count" | "a.insert_time" | "a.server_time" |\n
        "a.ws_books_count" | "a.ws_ambiguous_start_count" | "a.ws_ambiguous_end_count" |\n
        "a.bid_depth_diff" | "a.ask_depth_diff" | "a.bids_distance" | "a.asks_distance" |\n
        "a.ws_tick_changes_count" | "a.ws_tick_changes" | "a.tick_size_distance"\n
        <number> ::= [0-9]+ ("." [0-9]+)?\n
    \n
    If a syntax error is detected the change can not be applied.
    `
    const input_panel = document.createElement('div');
    input_panel.classList.add('attributes-panel');
    let is_valid_input = true;
    const apply_btn = document.createElement('button');
    apply_btn.textContent = "Apply";
    apply_btn.disabled = false;
    apply_btn.classList.add('attributes-apply-btn');

    const input_el = document.createElement('input');
    input_el.value = state.attributes || '';
    input_el.classList.add('attributes-input');

    input_el.addEventListener('input', () => {
        is_valid_input = validate_attributes_input(input_el.value);
        apply_btn.disabled = !is_valid_input;
        input_el.style.borderColor = is_valid_input ? 'green' : 'red';
    });

    apply_btn.addEventListener('click', () => {
        if (!is_valid_input) return;
        state.attributes = input_el.value.trim();
        console.log("Updated attributes:", state.attributes);
        apply_btn.textContent = "Applied";
        setTimeout(() => apply_btn.textContent = "Apply", 1000);
    });

    input_panel.append(input_el, apply_btn);
    panel.append(docs, input_panel);
}

function render_popup(render_panel) {
    const overlay = document.createElement('div');
    overlay.classList.add('popup-overlay');
    const window = document.createElement('div');
    window.classList.add('popup-window');
    const top_bar = document.createElement('div');
    top_bar.classList.add('popup-topbar');
    const close_btn = document.createElement('button');
    close_btn.textContent = "Close";
    close_btn.classList.add('popup-close-btn');
    close_btn.addEventListener('click', () => {
        document.body.removeChild(overlay);
    });
    const panel_content = document.createElement('div');
    panel_content.classList.add('popup-content');
    render_panel(panel_content);
    top_bar.appendChild(close_btn);
    overlay.append(top_bar, panel_content);
    document.body.appendChild(overlay);
}

function render_top_bar() {
    const bar = document.createElement('div');
    bar.classList.add('top-bar');

    const select_by = document.createElement('button');
    select_by.textContent = 'Attributes';
    select_by.classList.add('top-btn', 'attributes-btn');
    select_by.addEventListener('click', () => render_popup(render_attributes_panel));

    const filter_by = document.createElement('button');
    filter_by.textContent = 'Filter';
    filter_by.classList.add('top-btn', 'filter-btn');
    filter_by.addEventListener('click', () => render_popup(render_filter_panel));

    const order_by = document.createElement('button');
    order_by.textContent = 'Order';
    order_by.classList.add('top-btn', 'order-btn');
    order_by.addEventListener('click', () => render_popup(render_order_panel));

    const group_by = document.createElement('button');
    group_by.textContent = 'Grouping';
    group_by.classList.add('top-btn', 'grouping-btn');
    group_by.addEventListener('click', () => render_popup(render_grouping_panel));

    bar.append(select_by, filter_by, order_by, group_by);
    document.body.appendChild(bar)
}

function render_bottom_bar() {
    let is_valid_input = false;
    const bar = document.createElement('div');
    bar.classList.add('bottom-bar');

    const overview_panel = document.createElement('div');
    overview_panel.classList.add('overview-panel');

    const overview_info = document.createElement('div');
    overview_info.classList.add('overview-info');
    overview_info.textContent = `${state.offset} out of ${state.max_offset || 0}`;

    const prev_page = document.createElement('button');
    prev_page.textContent = "Prev";
    prev_page.classList.add('page-btn', 'prev-btn');

    const next_page = document.createElement('button');
    next_page.textContent = "Next";
    next_page.classList.add('page-btn', 'next-btn');

    update_page_overview = () => {
        const current_page = Math.floor(state.offset / tokens_per_page) + 1;
        const total_pages = Math.ceil((state.max_offset + 1) / tokens_per_page);
        overview_info.textContent = `page ${current_page} out of ${total_pages}`;
        prev_page.disabled = state.offset === 0;
        next_page.disabled = state.offset + tokens_per_page > state.max_offset;
    }

    prev_page.addEventListener('click', async () => {
        if (state.offset === 0) return;
        state.offset -= tokens_per_page;
        if (state.offset < 0) state.offset = 0;
        await refresh_row();
    });

    next_page.addEventListener('click', async () => {
        if (state.offset + tokens_per_page >= state.max_offset) return;
        state.offset += tokens_per_page;
        if (state.offset + tokens_per_page > state.max_offset) {
            state.offset = Math.max(0, state.max_offset - tokens_per_page);
        }
        await refresh_row();
    });

    overview_panel.append(prev_page, overview_info, next_page);

    const input_panel = document.createElement('div');
    input_panel.classList.add('offset-input-panel');

    const input_label = document.createElement('div');
    input_label.textContent = "Change offset:";
    input_label.classList.add('offset-label');

    const input_offset = document.createElement('input');
    input_offset.type = "number";
    input_offset.min = 0;
    input_offset.value = state.offset;
    input_offset.classList.add('offset-input');

    const submit_btn = document.createElement('button');
    submit_btn.textContent = "Load";
    submit_btn.classList.add('offset-submit-btn');

    input_offset.addEventListener('input', () => {
        const val = parseInt(input_offset.value, 10);
        const total_pages = Math.ceil((state.max_offset + 1) / tokens_per_page);
        if (!isNaN(val) && val >= 1 && val <= total_pages) {
            is_valid_input = true;
            submit_btn.disabled = false;
        } else {
            is_valid_input = false;
            submit_btn.disabled = true;
        }
    });

    submit_btn.addEventListener('click', async () => {
        if (!is_valid_input) return;
        const page = parseInt(input_offset.value, 10);
        state.offset = (page - 1) * tokens_per_page;
        await refresh_row();
    });

    input_panel.append(input_label, input_offset, submit_btn);

    bar.append(overview_panel, input_panel);
    document.body.appendChild(bar);

    update_page_overview();
}

function token_rows_init(rows_panel) {
    state.max_offset = 100
    rows_panel.classList.add('rows-panel');
    for (let i = 0; i < tokens_per_page; i++) {
        const row_el = document.createElement('div');
        row_el.classList.add('token-row');
        token_rows[i] = new token_row(row_el);
        row_el.textContent = `${i}`;
        rows_panel.appendChild(row_el);
    }
}

function dom_init() {
    render_top_bar();
    const rows_panel = document.createElement('div');
    token_rows_init(rows_panel);
    document.body.appendChild(rows_panel);
    render_bottom_bar();
    refresh_rows();
}
dom_init();
