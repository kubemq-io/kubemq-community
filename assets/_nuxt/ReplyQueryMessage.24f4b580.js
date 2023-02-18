import T from"./InputDropbox.b713c747.js";import A from"./InputText.a5d51f5e.js";import B from"./CodeEditor.a3b038a7.js";import{u as I}from"./request.fce72d22.js";import{u as Q}from"./useToastMessage.47adc00c.js";import{C as h}from"./send_response.a4e33ce2.js";import{a as U}from"./cqrs_request.ce51b25c.js";import{a as F,s as d,L as N,v as j,o as u,f as c,h as t,i as a,q as _,w as V,O as L,E as O,r as g,k as $}from"./entry.7307fc4c.js";import"./StatStripCard.vue_vue_type_style_index_0_scoped_b87ef05f_lang.ef632115.js";import"./stream.cbe555aa.js";import"./composables.0bc75d07.js";const D={class:"grid-nogutter grid col-12 mt-3"},W={class:"flex align-items-end grid p-fluid col-12 pb-0 mb-1"},z={class:"col-12 md:col-3 pt-0"},G={key:0,class:"col-12 md:col-6 pt-0"},H={key:0,class:"flex align-items-end grid p-fluid col-12 pb-0 mt-2 mb-1"},J={class:"col-12 md:col-12 pt-0"},K={key:1,class:"flex align-items-end grid p-fluid col-12 pb-0 mb-2 mt-2"},P={class:"col-12 md:col-12 pt-0"},X={class:"flex align-items-end grid p-fluid border-top-1 border-light-grey col-12 pb-0 mb-1"},Y={class:"col-12 md:col-6 pt-0"},Z={class:"col-12 md:col-6 pt-0"},ee={class:"flex align-items-end grid justify-content-end p-fluid col-12 mb-1 mt-2 pr-1"},oe={class:"mr-2"},le={class:"ml-2"},te=F({__name:"ReplyQueryMessage",setup(se){const f=L("dialogRef"),m=d();let s=N(new h);const o=d({body:"",metadata:"",tags:""}),n=d("executed"),r=d(""),C=d([{label:"Executed",value:"executed"},{label:"Error",value:"error"}]);j(()=>{m.value=f.value.data.message});const R=I(),p=Q(),i=d(!1),k=()=>{i.value=!0,O(()=>{i.value=!0}),s=new h,s.requestId=m.value.requestId,s.replyChannel=m.value.replyChannel,s.body=o.value.body,s.metadata=o.value.metadata,s.tags=o.value.tags,n.value==="error"?s.error=r.value:s.executed=!0;const b=new U(s);R.sendRequest(b).then(e=>{p.showSuccess("Reply sent successfully")}).catch(e=>{e.isCanceled?p.showWarn("Request Canceled"):p.showError("Error sending reply for query message",e.message)}).finally(()=>{i.value=!1,y()})},y=()=>{f.value.close()},w=()=>n.value==="error"?r.value.length>0:o.value.body.length>0;return(b,e)=>{const q=T,v=A,E=B,M=g("AccordionTab"),S=g("Accordion"),x=g("Button");return u(),c("div",D,[t("div",W,[t("div",z,[a(q,{label:"Reply Query",modelValue:n.value,"onUpdate:modelValue":e[0]||(e[0]=l=>n.value=l),options:C.value},null,8,["modelValue","options"])]),n.value==="error"?(u(),c("div",G,[a(v,{modelValue:r.value,"onUpdate:modelValue":e[1]||(e[1]=l=>r.value=l),label:"Error Text",placeholder:"Error description"},null,8,["modelValue"])])):_("",!0)]),n.value==="executed"?(u(),c("div",H,[t("div",J,[a(E,{label:"Body*",modelValue:o.value.body,"onUpdate:modelValue":e[2]||(e[2]=l=>o.value.body=l),height:"300px"},null,8,["modelValue"])])])):_("",!0),n.value==="executed"?(u(),c("div",K,[t("div",P,[a(S,null,{default:V(()=>[a(M,{header:"Message Attributes"},{default:V(()=>[t("div",X,[t("div",Y,[a(v,{label:"Metadata",modelValue:o.value.metadata,"onUpdate:modelValue":e[3]||(e[3]=l=>o.value.metadata=l)},null,8,["modelValue"])]),t("div",Z,[a(v,{label:"Tags",modelValue:o.value.tags,"onUpdate:modelValue":e[4]||(e[4]=l=>o.value.tags=l),placeholder:"key=value,key2=value2"},null,8,["modelValue"])])])]),_:1})]),_:1})])])):_("",!0),t("div",ee,[t("div",oe,[a(x,{label:"Cancel",class:"button-grey",icon:"pi pi-times",onClick:e[5]||(e[5]=l=>y()),style:{width:"10rem"}})]),t("div",le,[a(x,{label:"Reply",class:"button-grey",loading:i.value,icon:"pi pi-reply",style:{width:"10rem"},onClick:k,disabled:!w()},null,8,["loading","disabled"])])])])}}}),ge=$(te,[["__scopeId","data-v-eb1573ac"]]);export{ge as default};