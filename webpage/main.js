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
        const response = await fetch("http://172.16.0.114:8080/refresh", {
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

function init_analytics_tab() {

}

function init_token_events_tab() {

}

function init_token_refs_tab() {

}



function init_token_details_panel() {
    const token_details_panel = document.createElement('div');
    const tab_selection = document.createElement('div');
    const refs_tab = document.createElement('button');
    const events_tab = document.createElement('button');
    const analytics_tab = document.createElement('button');
    const tab_content = document.createElement('div');
    tab_selection.append(refs_tab, events_tab, analytics_tab);
    token_details_panel.append(tab_selection, tab_content);
    return token_details_panel;
}
const analytics_tab = null;

const refs_tab = new HTMLMapElement();

panel = init_token_details_panel();

function render_token_details_panel(tab) {
    tab_selection = panel.getEleme // don't know syntax
    if (tab === analytics_tab) {
        
    }
}

function init_token_selection_panel() {
    const order = {
        "attribute":"resubs_since_last_book",
        "ascending":true,
        "nulls_last":true
    }
    const filters = {
        "token_id":{
            "include_null":true,
            "filter_values":null
        },
        "resubs_since_last_book":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        },
        "events_since_last_resub":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        },
        "books_since_last_resub":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        },
        "changes_since_last_resub":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        },
        "tick_changes_since_last_resub":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        },
        "bids_distance":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        },
        "asks_distance":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        },
        "bids_depth_diff":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        },
        "asks_depth_diff":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        },
        "tick_change_distance":{
            "include_null":true,
            "filter_values":null,
            "lower_bound":{
                "inclusive":true,
                "value":null
            },
            "upper_bound":{
                "inclusive":true,
                "value":null
            }
        }
    };
    const selection_panel = document.createElement('div');
    const attribute_selection_panel = document.createElement('div');
    class token_attribute {
        constructor(attribute) {
            this.name_div = document.createElement('div');
            this.name_div.textContent = attribute;


            this.include_null_div = document.createElement('div');
            const include_null = document.createElement('input');
            include_null.type = 'checkbox';
            include_null.checked = filters[attribute]["include_null"];
            include_null.addEventListener('change', () => {
                filters[attribute]["include_null"] = include_null.checked;
            });
            this.include_null_div.appendChild(include_null);
            
            this.filter_vals_div = document.createElement('div');
            const is_valid_vals_filter = true;
            const filter_vals = document.createElement('input');
            const filter_vals_btn = document.createElement('button');
            filter_vals_btn.textContent = "Apply";
            filter_vals.addEventListener('input', () => {
                if (!(/^\s*[^ ,][^ ,]*\s*(\s*,\s*[^ ,][^ ,]*\s*)*$/.test(filter_vals.value))) {
                    this.is_valid_vals_filter = false;
                    // show visually clickable apply
                    return;
                }
                this.is_valid_vals_filter = true;
                // show visually unclickable apply
            });
            this.filter_vals_div.append(filter_vals, filter_vals_btn);

            this.lower_bound_div = document.createElement('div');
            this.upper_bound_div = document.createElement('div');
            this.order_by_div = document.createElement('div');
        } 
    }
    class orderable_token_attribute extends token_attribute{
        static curr_order_by = null;
        constructor(name) {
            super(name);
            
            const lb_inclusive_btn = document.createElement('button');
            lb_inclusive_btn.textContent = "[";
            lb_inclusive_btn.addEventListener('click', () => {
                if(filters[name]["lower_bound"]["inclusive"])
                    lb_inclusive_btn.textContent = "(";
                else {
                    lb_inclusive_btn.textContent = "[";
                }
                filters[name]["lower_bound"]["inclusive"] = !filters[name]["lower_bound"]["inclusive"];
            });
            const lower_bound = document.createElement('input');
            lower_bound.value = "-INF";
            lower_bound.addEventListener('input', () => {     
                const val = lower_bound.value.trim();
                if (/^-?\d+$/.test(val)) {
                    filters[name]["lower_bound"]["value"] = parseInt(val, 10);
                } else if (/^-?\d*\.\d+$/.test(val)) {
                    filters[name]["lower_bound"]["value"] = parseFloat(val);
                } else if (/^-\s*inf(?:inity)?$/i.test(val)) {
                    lower_bound.value = "-INF";
                    filters[name]["lower_bound"]["value"] = null;
                }
            });
            this.lower_bound_div.append(lb_inclusive_btn, lower_bound);

            const ub_inclusive_btn = document.createElement('button');
            ub_inclusive_btn.textContent = ']';
            ub_inclusive_btn.addEventListener('click', () => {
                if(filters[name]["upper_bound"]["inclusive"])
                    ub_inclusive_btn.textContent = "(";
                else {
                    ub_inclusive_btn.textContent = "[";
                }
                filters[name]["upper_bound"]["inclusive"] = !filters[name]["upper_bound"]["inclusive"];
            });
            const upper_bound = document.createElement('input');
            upper_bound.value = "INF";
            upper_bound.addEventListener('input', () => {     
                const val = upper_bound.value.trim();
                if (/^-?\d+$/.test(val)) {
                    filters[name]["upper_bound"]["value"] = parseInt(val, 10);
                } else if (/^-?\d*\.\d+$/.test(val)) {
                    filters[name]["upper_bound"]["value"] = parseFloat(val);
                } else if (/\s*inf(?:inity)?$/i.test(val)) {
                    upper_bound.value = "INF";
                    filters[name]["upper_bound"]["value"] = null;
                }
            });
            this.upper_bound_div.append(upper_bound, ub_inclusive_btn);
            
            const sort_by = document.createElement('input');
            sort_by.type = 'checkbox';
            sort_by.addEventListener('change', () => { 
                if (orderable_token_attribute.curr_order_by)
                    orderable_token_attribute.curr_order_by.checked = false;
                orderable_token_attribute.curr_order_by = sort_by;
              order["attribute"] = name;
            });
            this.sort_by_div.appendChild(sort_by);
            if (name === "resubs_since_last_book") {
                orderable_token_attribute.curr_order_by = sort_by;
            }
        }
    }
    const labels_row = document.createElement('div');
    const attributes_lbl = document.createElement('div');
    attributes_lbl.textContent = "Attribute";
    const include_null_lbl = document.createElement('div');
    include_null_lbl.textContent = "Include Null";
    const filter_vals_lbl = document.createElement('div');
    filter_vals_lbl.textContent = "Filter Values";
    const lower_bound_lbl = document.createElement('div');
    lower_bound_lbl.textContent = "Lower Bound";
    const upper_bound_lbl = document.createElement('div');
    upper_bound_lbl.textContent = "Upper Bound";
    const order_by_lbl = document.createElement('div');
    order_by_lbl.textContent = "Sort By";
    labels_row.append(attributes_lbl, include_null_lbl, filter_vals_lbl, lower_bound_lbl, upper_bound_lbl, order_by_lbl);
    attribute_selection_panel.appendChild(labels_row);

    for (const attribute in filters) {
        const attribute_row = document.createElement('div');
        let token_attr = null;
        if ("lower_bound" in filters[attribute]) {
            token_attr = new orderable_token_attribute(attribute);
        } else {
            token_attr = new token_attribute(attribute);
        }
        attribute_row.append(
            token_attr.name_div, 
            token_attr.include_null_div,
            token_attr.filter_vals_div,
            token_attr.lower_bound_div,
            token_attr.upper_bound_div,
            token_attr.order_by_div
        )
        attribute_selection_panel.appendChild(attribute_row);
    }
    
    const order_by_div = document.createElement('div');
    const order_by_options_lbl = document.createElement('div');
    order_by_options_lbl.textContent = "Sorting options:"
    
    const order_selection_div = document.createElement('div');
    const ascending_btn = document.createElement('button'); // initially viusally selected
    const descending_btn = document.createElement('button'); // initially visually unselected
    ascending_btn.textContent = "Ascending";
    ascending_btn.addEventListener('click', () => {
        if(order["ascending"]) {
            return;
        }
        order["ascending"] = true;
        // change visuals of both buttons
    });
    descending_btn.textContent = "Descending";
    descending_btn.addEventListener('click', () => {
        if(!order["ascending"]) {
            return;
        }
        order["ascending"] = false;
        // change visuals of both buttons
    });
    order_selection_div.append(ascending_btn, descending_btn);

    const null_order_selection_div = document.createElement('div');
    const nulls_last_btn = document.createElement('button'); // initially viusally selected
    const nulls_first_btn = document.createElement('button'); // initially visually unselected
    nulls_last_btn.textContent = "Nulls Last";
    nulls_last_btn.addEventListener('click', () => {
        if(order["nulls_last"]) {
            return;
        }
        order["nulls_last"] = true;
        // change visuals of both buttons
    });
    nulls_first_btn.textContent = "Nulls First";
    nulls_first_btn.addEventListener('click', () => {
        if(!order["nulls_last"]) {
            return;
        }
        order["nulls_last"] = false;
        // change visuals of both buttons
    });
    null_order_selection_div.append(nulls_last_btn, nulls_first_btn);
    
    order_by_div.append(order_by_options_lbl, order_selection_div, null_order_selection_div);

    selection_panel.append(attribute_selection_panel, order_by_div);
    
    return selection_panel;
}

function init_popup() {
    // overlay should cover/blur the entire bg
    const overlay = document.createElement('div');
    const window = document.createElement('div');
    const top_bar = document.createElement('div');

    const close_btn = document.createElement('button');
    close_btn.textContent = "Close";
    close_btn.addEventListener('click', () => {
        document.body.removeChild(overlay);
        window.removeChild(content);
        
    });
    panel_content.classList.add('popup-content');
    top_bar.appendChild(close_btn);
    window.appendChild(top_bar);
    overlay.append(window);
    return overlay;
}

function render_popup(popup, content) {
    const window = popup.getElement(window);
    window.appendChild(content);
    document.body.appendChild(popup);
}

function render_top_bar(token_selection_panel) {
    const bar = document.createElement('div');
    const token_selection_btn = document.createElement('button');
    token_selection_btn.textContent = 'Token';
    token_selection_btn.addEventListener('click', () => render_popup(token_selection_panel));

    bar.appendChild(token_selection_btn);
    document.body.appendChild(bar);
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
    const popup = init_popup()
    const token_selection_panel = init_token_selection_panel()
    render_top_bar(token_selection_panel);
    const rows_panel = document.createElement('div');
    token_rows_init(rows_panel);
    document.body.appendChild(rows_panel);
    render_bottom_bar();
    refresh_rows();
}
dom_init();
