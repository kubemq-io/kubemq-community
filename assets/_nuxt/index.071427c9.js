import u from"./ActionButtons.5ac539b3.js";import b from"./StatStripCard.9c4ee5f2.js";import{_ as h}from"./Channels.vue_vue_type_script_setup_true_lang.1392190d.js";import{a as x,b as f,o,h as n,i as e,u as t,j as c,t as v,s as y,r as g,p as k,k as S,l as w}from"./entry.86b9e19f.js";import"./AddChannel.vue_vue_type_script_setup_true_lang.86041a51.js";import"./InputText.0abbd94c.js";import"./index.esm.81d2c330.js";import"./request.29e64ff5.js";import"./useToastMessage.bad030a2.js";import"./SendMessage.bcbf7528.js";import"./CodeEditor.f8fdf1e5.js";import"./StatStripCardBox.818f187b.js";const C=a=>(k("data-v-58699d6c"),a=a(),S(),a),V={class:"flex flex-column container"},I={class:"flex justify-content-between"},B={class:"flex flex-column header-row"},L=C(()=>e("div",{class:"flex align-items-center mb-1"},[e("span",{class:"material-symbols-outlined mr-1 text-4xl"},"hub"),e("span",{class:"text-4xl"},"PubSub")],-1)),j={key:0},A={key:1,class:"flex"},D={key:0,class:"material-symbols-outlined text-pubsub mr-1 text-lg"},N={key:1,class:"material-symbols-outlined text-grey mr-1 text-lg"},P={class:"ml-1"},R={key:0,class:"flex justify-content-end align-items-center"},E={class:"stats-row"},U={class:"flex-grow-1 channels-parent"},$={class:"bg-white border-round-2xl border-1 border-pubsub"},q=x({__name:"index",setup(a){const{pubsubData:s,isStreamReady:l}=f();return(z,i)=>{const r=g("Skeleton"),_=u,d=b,m=h;return o(),n("div",V,[e("div",I,[e("div",B,[L,t(l)?(o(),n("div",A,[t(s).isActive?(o(),n("span",D,"radio_button_checked")):(o(),n("span",N,"radio_button_unchecked")),e("span",P,"Last Activity: "+v(t(s).lastSeen),1)])):(o(),n("div",j,[c(r,{height:"0.9rem",width:"13rem",borderRadius:"12px"})]))]),t(s).channelsList.length>0?(o(),n("div",R,[c(_)])):y("",!0)]),e("div",E,[c(d,{items:t(s).stats.Items,"background-color":"bg-pubsub","border-color":"border-pubsub"},null,8,["items"])]),e("div",U,[e("div",$,[c(m,{modelValue:t(s).channelsList,"onUpdate:modelValue":i[0]||(i[0]=p=>t(s).channelsList=p)},null,8,["modelValue"])])])])}}});const Z=w(q,[["__scopeId","data-v-58699d6c"]]);export{Z as default};