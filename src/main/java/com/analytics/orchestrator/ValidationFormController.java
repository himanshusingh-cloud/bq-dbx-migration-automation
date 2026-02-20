package com.analytics.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serves the validation form at / with API groups rendered server-side.
 * Guarantees API list and form work without client-side JS dependencies.
 */
@Controller
public class ValidationFormController {

    private static final ObjectMapper JSON = new ObjectMapper();

    @GetMapping(value = {"", "/", "/validation-form"}, produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<String> form() throws JsonProcessingException {
        Map<String, List<String>> groups = new LinkedHashMap<>();
        groups.put("analytics", ValidationService.PRODUCT_CONTENT_APIS);
        groups.put("multiLocation2.0", ValidationService.MULTI_LOCATION_APIS);
        groups.put("search", ValidationService.SEARCH_APIS);

        StringBuilder apiHtml = new StringBuilder();
        for (Map.Entry<String, List<String>> e : groups.entrySet()) {
            apiHtml.append("<div id=\"api-group-").append(e.getKey()).append("\" class=\"api-group api-grid\" data-group=\"").append(e.getKey()).append("\">");
            for (String api : e.getValue()) {
                apiHtml.append("<label class=\"api-option\"><input type=\"checkbox\" name=\"apis\" value=\"").append(escape(api)).append("\"> <span>").append(escape(api)).append("</span></label>");
            }
            apiHtml.append("</div>");
        }

        String groupsJson = JSON.writeValueAsString(groups);

        String html = "<!DOCTYPE html>\n" +
                "<html><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n" +
                "<title>Analytics API Validation</title>\n" +
                "<style>\n" +
                "body{font-family:system-ui,sans-serif;background:#0a0e14;color:#e6edf3;margin:0;padding:2rem;}\n" +
                ".c{max-width:960px;margin:0 auto;}\n" +
                "h1{color:#00d4aa;font-size:1.5rem;}\n" +
                ".sub{color:#8b949e;font-size:0.9rem;margin-bottom:1.5rem;}\n" +
                ".grid{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1rem;}\n" +
                ".fld{display:flex;flex-direction:column;gap:0.3rem;}\n" +
                ".fld label{font-size:0.75rem;color:#8b949e;text-transform:uppercase;}\n" +
                "input,select{background:#1a2332;border:1px solid #2d3748;border-radius:6px;padding:0.5rem 0.75rem;color:#e6edf3;font-size:0.9rem;}\n" +
                ".api-box{background:#131920;border:1px solid #2d3748;border-radius:8px;padding:1rem;margin:1rem 0;}\n" +
                ".api-title{font-size:0.8rem;color:#8b949e;margin-bottom:0.75rem;}\n" +
                ".api-actions{margin-bottom:0.5rem;}\n" +
                ".btn-sm{background:#1a2332;border:1px solid #2d3748;color:#8b949e;padding:0.3rem 0.6rem;border-radius:4px;cursor:pointer;font-size:0.8rem;margin-right:0.5rem;}\n" +
                ".btn-sm:hover{border-color:#00d4aa;color:#00d4aa;}\n" +
                ".api-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:0.4rem;}\n" +
                ".api-option{display:flex;align-items:center;gap:0.5rem;padding:0.4rem 0.6rem;background:#1a2332;border:1px solid #2d3748;border-radius:4px;cursor:pointer;font-size:0.85rem;}\n" +
                ".api-option:hover{border-color:#00d4aa;}\n" +
                ".api-option input{width:16px;height:16px;accent-color:#00d4aa;}\n" +
                ".api-group{display:none;}\n" +
                ".api-group.active{display:block;}\n" +
                ".btn-run{background:#00d4aa;color:#0a0e14;border:none;padding:0.75rem 1.5rem;border-radius:8px;font-size:1rem;font-weight:600;cursor:pointer;margin-top:1rem;}\n" +
                ".btn-run:hover{background:#00f5c4;}\n" +
                ".btn-run:disabled{opacity:0.6;cursor:not-allowed;}\n" +
                ".res{display:none;margin-top:1.5rem;background:#131920;border:1px solid #2d3748;border-radius:8px;padding:1rem;}\n" +
                ".res.visible{display:block;}\n" +
                ".res pre{background:#0d1117;padding:1rem;border-radius:4px;font-size:0.8rem;overflow:auto;white-space:pre-wrap;}\n" +
                ".err{color:#ff6b6b;}\n" +
                "</style></head><body><div class=\"c\">\n" +
                "<h1>Analytics API Validation</h1>\n" +
                "<p class=\"sub\">POST /api/run-validation-tests — same as curl</p>\n" +
                "<form id=\"f\" novalidate>\n" +
                "<div class=\"grid\">\n" +
                "<div class=\"fld\"><label>client</label><input type=\"text\" id=\"client\" placeholder=\"mondelez-fr\" required></div>\n" +
                "<div class=\"fld\"><label>environment</label><select id=\"env\"><option value=\"test\">test</option><option value=\"staging\">staging</option><option value=\"prod\">prod</option></select></div>\n" +
                "<div class=\"fld\"><label>apiGroup</label><select id=\"apiGroup\"><option value=\"analytics\">analytics</option><option value=\"multiLocation2.0\">multiLocation2.0</option><option value=\"search\">search</option></select></div>\n" +
                "<div class=\"fld\"><label>startDate</label><input type=\"text\" id=\"startDate\" value=\"2026-02-01\"></div>\n" +
                "<div class=\"fld\"><label>endDate</label><input type=\"text\" id=\"endDate\" value=\"2026-02-09\"></div>\n" +
                "</div>\n" +
                "<div class=\"api-box\">\n" +
                "<div class=\"api-title\">apis (leave all unchecked = run all)</div>\n" +
                "<div class=\"api-actions\"><button type=\"button\" class=\"btn-sm\" id=\"selAll\">Select All</button><button type=\"button\" class=\"btn-sm\" id=\"selNone\">Select None</button></div>\n" +
                "<div id=\"apiContainer\">" + apiHtml + "</div>\n" +
                "</div>\n" +
                "<button type=\"submit\" class=\"btn-run\" id=\"runBtn\">Run Validation</button>\n" +
                "</form>\n" +
                "<div class=\"res\" id=\"res\"><h3>Response</h3><pre id=\"resJson\"></pre></div>\n" +
                "</div>\n" +
                "<script>\n" +
                "var GROUPS=" + groupsJson + ";\n" +
                "function showGroup(g){document.querySelectorAll('.api-group').forEach(function(d){d.classList.toggle('active',d.dataset.group===g);});}\n" +
                "document.getElementById('apiGroup').onchange=function(){showGroup(this.value);};\n" +
                "showGroup(document.getElementById('apiGroup').value);\n" +
                "document.getElementById('selAll').onclick=function(){var g=document.getElementById('apiGroup').value;document.querySelectorAll('[data-group=\"'+g+'\"] input').forEach(function(c){c.checked=true;});};\n" +
                "document.getElementById('selNone').onclick=function(){var g=document.getElementById('apiGroup').value;document.querySelectorAll('[data-group=\"'+g+'\"] input').forEach(function(c){c.checked=false;});};\n" +
                "function qs(){var p={};location.search.slice(1).split('&').forEach(function(x){var k=x.split('=');if(k[1])p[k[0]]=decodeURIComponent(k[1]);});return p;}\n" +
                "var q=qs();if(q.client)document.getElementById('client').value=q.client;if(q.environment)document.getElementById('env').value=q.environment||'test';if(q.apiGroup){document.getElementById('apiGroup').value=q.apiGroup;showGroup(q.apiGroup);}if(q.startDate)document.getElementById('startDate').value=q.startDate;if(q.endDate)document.getElementById('endDate').value=q.endDate;\n" +
                "document.getElementById('f').onsubmit=function(e){e.preventDefault();var client=document.getElementById('client').value.trim();if(!client){document.getElementById('resJson').textContent='Error: client is required';document.getElementById('res').classList.add('visible');return;}var btn=document.getElementById('runBtn');btn.disabled=true;btn.textContent='Running...';document.getElementById('res').classList.remove('visible');\n" +
                "var g=document.getElementById('apiGroup').value;var apis=[];document.querySelectorAll('[data-group=\"'+g+'\"] input:checked').forEach(function(c){apis.push(c.value);});\n" +
                "var pl={client:document.getElementById('client').value.trim(),environment:document.getElementById('env').value,apiGroup:g,startDate:document.getElementById('startDate').value.trim(),endDate:document.getElementById('endDate').value.trim()};if(apis.length>0)pl.apis=apis;\n" +
                "var body=JSON.stringify(pl);\n" +
                "var xhr=new XMLHttpRequest();xhr.open('POST','/api/run-validation-tests');xhr.setRequestHeader('Content-Type','application/json');\n" +
                "xhr.onload=function(){btn.disabled=false;btn.textContent='Run Validation';var d;try{d=JSON.parse(xhr.responseText);}catch(err){document.getElementById('resJson').textContent='Error: '+xhr.responseText;document.getElementById('res').classList.add('visible');return;}\n" +
                "document.getElementById('resJson').textContent=JSON.stringify(d,null,2);document.getElementById('res').classList.add('visible');if(d&&d.suiteId&&xhr.status>=200&&xhr.status<300){location.href='/validation-report/'+d.suiteId;};\n" +
                "xhr.onerror=function(){btn.disabled=false;btn.textContent='Run Validation';document.getElementById('resJson').textContent='Network error. Is server running?';document.getElementById('res').classList.add('visible');};\n" +
                "xhr.send(body);};\n" +
                "</script></body></html>";

        return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(html);
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
