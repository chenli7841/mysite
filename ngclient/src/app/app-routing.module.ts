import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { DemoComponent } from './demo/demo.component';
import { ProfileComponent } from './profile/profile.component';


const routes: Routes = [
  { path: '', component: ProfileComponent },
  { path: 'demo', component: DemoComponent }];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
