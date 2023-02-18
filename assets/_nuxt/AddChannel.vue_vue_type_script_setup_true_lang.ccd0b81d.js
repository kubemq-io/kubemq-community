import I from"./InputText.a5d51f5e.js";import{a as U,v as A,O as M,s as b,o as C,f as y,h as e,i as c,u as i,A as f,q as x,M as Q,r as R}from"./entry.7307fc4c.js";import{u as j,r as G}from"./index.esm.ec6caf95.js";import{G as m,u as F}from"./request.fce72d22.js";import{u as O}from"./useToastMessage.47adc00c.js";import{a as S,b as P}from"./stream.cbe555aa.js";class z extends m{constructor(l){super("create_channel",{type:"queues",name:l})}}class D extends m{constructor(l){super("create_channel",{type:"events",name:l})}}class H extends m{constructor(l){super("create_channel",{type:"events_store",name:l})}}class J extends m{constructor(l){super("create_channel",{type:"commands",name:l})}}class K extends m{constructor(l){super("create_channel",{type:"queries",name:l})}}const L={class:"grid-nogutter grid col-12 mt-2"},W={class:"flex align-items-end grid p-fluid col-6 pb-0 mb-1"},X={class:"col-12 md:col-12 pt-0"},Y={key:0,class:"flex grid p-fluid col-6 ml-2"},Z=e("div",{class:"col-12 md:col-12 pt-0"},[e("label",{class:"text-sm ml-1 font-medium"},"Select Channel Type")],-1),ee={class:"col-12 md:col-12 pt-0 mt-1"},se={class:"flex ml-1"},le={class:"mr-2"},te=e("label",{class:"ml-1",for:"events"},"Events",-1),ne={class:"ml-2"},oe=e("label",{class:"ml-1",for:"events_store"},"Events Store",-1),ae={key:1,class:"flex grid p-fluid flex grid p-fluid col-6 ml-2"},ue=e("div",{class:"col-12 md:col-12 pt-0"},[e("label",{class:"text-sm ml-1 font-medium"},"Select Channel Type")],-1),de={class:"col-12 md:col-12 pt-0 mt-1"},ce={class:"flex ml-1"},ie={class:"mr-2"},re=e("label",{class:"ml-1",for:"events"},"Commands",-1),me={class:"ml-2"},ve=e("label",{class:"ml-1",for:"events_store"},"Queries",-1),pe={class:"flex align-items-end grid justify-content-end p-fluid col-12 pb-0 mb-1 mt-3"},_e={class:"mr-2"},fe={class:"ml-2"},qe=U({__name:"AddChannel",setup(r){A(()=>{v.value=l.value.data.channelType});const l=M("dialogRef"),n=b(""),v=b(1),a=S("add-channel-pub-sub-radio-button","events"),u=S("add-channel-cqrs-radio-button","commands"),p=b(!1),w={channelName:{required:G}},k=P("system-connection"),B=d=>{d==="disconnected"&&h()};k.on(B);const V=j(w,{channelName:n}),E=F(),g=O();let o=null;const N=d=>{d&&T()},h=()=>{o!==null&&o.abort(),l.value.close()},T=()=>{switch(v.value){case 1:o=new z(n.value);break;case 2:a.value==="events"?o=new D(n.value):o=new H(n.value);break;case 3:u.value==="commands"?o=new J(n.value):o=new K(n.value);break}p.value=!0,E.sendRequest(o).then(d=>{g.showSuccess(`Channel '${n.value}' created successfully`),h()}).catch(d=>{g.showError("Error creating a new channel",d)}).finally(()=>{p.value=!1,o=null})};return(d,s)=>{const $=I,_=R("RadioButton"),q=R("Button");return C(),y("form",{onSubmit:s[6]||(s[6]=Q(t=>N(!i(V).$invalid),["prevent"]))},[e("div",L,[e("div",W,[e("div",X,[c($,{label:"Set Channel Name*",modelValue:n.value,"onUpdate:modelValue":s[0]||(s[0]=t=>n.value=t),placeholder:"Channel name",disabled:p.value},null,8,["modelValue","disabled"])])]),v.value===2?(C(),y("div",Y,[Z,e("div",ee,[e("div",se,[e("div",le,[c(_,{inputId:"events",name:"pubsub",value:"events",modelValue:i(a),"onUpdate:modelValue":s[1]||(s[1]=t=>f(a)?a.value=t:null)},null,8,["modelValue"]),te]),e("div",ne,[c(_,{inputId:"events_store",name:"pubsub",value:"events_store",modelValue:i(a),"onUpdate:modelValue":s[2]||(s[2]=t=>f(a)?a.value=t:null)},null,8,["modelValue"]),oe])])])])):x("",!0),v.value===3?(C(),y("div",ae,[ue,e("div",de,[e("div",ce,[e("div",ie,[c(_,{inputId:"commands",name:"cqrs",value:"commands",modelValue:i(u),"onUpdate:modelValue":s[3]||(s[3]=t=>f(u)?u.value=t:null)},null,8,["modelValue"]),re]),e("div",me,[c(_,{inputId:"queries",name:"cqrs",value:"queries",modelValue:i(u),"onUpdate:modelValue":s[4]||(s[4]=t=>f(u)?u.value=t:null)},null,8,["modelValue"]),ve])])])])):x("",!0),e("div",pe,[e("div",_e,[c(q,{label:"Cancel",onClick:s[5]||(s[5]=t=>h()),icon:"pi pi-times",class:"button-grey",style:{width:"8rem"}})]),e("div",fe,[c(q,{label:"Add",disabled:i(V).$invalid,type:"submit",icon:"pi pi-plus",class:"button-grey",loading:p.value,style:{width:"8rem"}},null,8,["disabled","loading"])])])])],32)}}});export{qe as _};